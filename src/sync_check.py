"""
sync_check.py
Periodic PostgreSQL ↔ Snowflake row-count reconciliation.

Used two ways:
  1. Imported and run in a background daemon thread by main.py, so the live
     service self-checks on a schedule (SYNC_CHECK_INTERVAL_H, default 24h).
  2. Run standalone as a one-shot check:
       python -m src.sync_check
     Exits 0 if every table is in sync, 1 if any table has drifted or the
     check itself failed — handy as a CI/cron gate.

Sends metrics + a warning event to Datadog when configured (DD_API_KEY set);
no-ops otherwise. See monitoring.py.

Metrics posted per table:
  cdc.sync.pg_row_count  cdc.sync.sf_row_count  cdc.sync.row_diff  cdc.sync.in_sync
"""

from __future__ import annotations

import logging
import os
import time
from typing import Dict

log = logging.getLogger("cdc.sync_check")

DRIFT_THRESHOLD: int = int(os.environ.get("SYNC_DRIFT_THRESHOLD", "100"))
SYNC_CHECK_INTERVAL_H: float = float(os.environ.get("SYNC_CHECK_INTERVAL_H", "24"))
_INITIAL_DELAY_S: int = int(os.environ.get("SYNC_CHECK_INITIAL_DELAY_S", "3600"))

# PG table → Snowflake current-state table
_TABLE_PAIRS = [
    ("public.customers",        "CUSTOMERS"),
    ("public.customer_devices", "CUSTOMER_DEVICES"),
]


# ── PostgreSQL count ────────────────────────────────────────────────────────── #

def _pg_count(pg_table: str) -> int:
    """Prefer the fast planner estimate; fall back to an exact COUNT(*)."""
    import psycopg2
    from .pg_replication import PG_HOST, PG_DATABASE, PG_USER, _get_pg_password

    conn = psycopg2.connect(
        host=PG_HOST, dbname=PG_DATABASE, user=PG_USER,
        password=_get_pg_password(), sslmode="require", connect_timeout=15,
    )
    try:
        cur = conn.cursor()
        schema, table = pg_table.split(".")
        cur.execute(
            "SELECT n_live_tup FROM pg_stat_user_tables WHERE schemaname=%s AND relname=%s",
            (schema, table),
        )
        row = cur.fetchone()
        approx = row[0] if row else 0
        if approx > 0:
            return int(approx)
        cur.execute(f"SELECT COUNT(*) FROM {pg_table}")  # table name is from the hardcoded list above
        return cur.fetchone()[0]
    finally:
        conn.close()


# ── Snowflake count ─────────────────────────────────────────────────────────── #

def _sf_count(sf_table: str) -> int:
    import snowflake.connector
    from .snowflake_sink import _SF_DATABASE, _SF_SCHEMA

    sink_creds = _fetch_sf_creds()
    conn = snowflake.connector.connect(
        account=sink_creds["account"], user=sink_creds["user"], password=sink_creds["password"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=_SF_DATABASE, schema=_SF_SCHEMA,
        login_timeout=60, network_timeout=120,
    )
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {_SF_DATABASE}.{_SF_SCHEMA}.{sf_table}")
        return cur.fetchone()[0]
    finally:
        conn.close()


def _fetch_sf_creds() -> Dict[str, str]:
    if os.environ.get("SNOWFLAKE_ACCOUNT"):
        return {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "password": os.environ["SNOWFLAKE_PASSWORD"],
        }
    from azure.identity import ManagedIdentityCredential
    from azure.keyvault.secrets import SecretClient

    kv_mi, kv_name = os.environ["SNOWFLAKE_KV_MI_CLIENT_ID"], os.environ["SNOWFLAKE_KV_NAME"]
    mi = ManagedIdentityCredential(client_id=kv_mi)
    kv = SecretClient(vault_url=f"https://{kv_name}.vault.azure.net", credential=mi)
    return {
        "account": kv.get_secret("SnowflakeETLAccount").value,
        "user": kv.get_secret("SnowflakeETLLogin").value,
        "password": kv.get_secret("SnowflakeETLPassword").value,
    }


# ── Main check ────────────────────────────────────────────────────────────── #

def run_sync_check() -> Dict[str, dict]:
    """Compare PG vs SF row counts for every tracked table.
    Returns a dict keyed by Snowflake table name: {pg, sf, diff, in_sync}."""
    from .monitoring import DD_ENV, send_event_to_datadog, send_metric_to_datadog

    results: Dict[str, dict] = {}
    for pg_table, sf_table in _TABLE_PAIRS:
        table_label = sf_table.lower()
        log.info("Counting rows", extra={"pg_table": pg_table, "sf_table": sf_table})

        pg_count = sf_count = diff = 0
        ok = True
        try:
            pg_count = _pg_count(pg_table)
            sf_count = _sf_count(sf_table)
            diff = abs(pg_count - sf_count)
            in_sync = diff <= DRIFT_THRESHOLD
        except Exception as exc:
            log.error("Sync check failed for table", extra={"table": sf_table, "error": str(exc)}, exc_info=True)
            ok = False
            in_sync = False

        tags = [f"table:{table_label}"]
        if ok:
            send_metric_to_datadog("cdc.sync.pg_row_count", pg_count, tags)
            send_metric_to_datadog("cdc.sync.sf_row_count", sf_count, tags)
            send_metric_to_datadog("cdc.sync.row_diff", diff, tags)
            send_metric_to_datadog("cdc.sync.in_sync", 1.0 if in_sync else 0.0, tags)
            log.info("Sync check result", extra={
                "table": sf_table, "pg_count": pg_count, "sf_count": sf_count,
                "diff": diff, "in_sync": in_sync, "threshold": DRIFT_THRESHOLD,
            })
            if not in_sync:
                send_event_to_datadog(
                    title=f"[pg-snowflake-cdc] {sf_table} out of sync ({DD_ENV})",
                    text=(
                        f"PostgreSQL has **{pg_count:,}** rows, Snowflake has **{sf_count:,}** rows — "
                        f"drift of **{diff:,}** rows (threshold {DRIFT_THRESHOLD:,}).\n"
                        "Check replication slot lag, or run scripts/reconcile.py."
                    ),
                    alert_type="warning", tags=tags,
                )
                log.warning("Row count drift exceeds threshold", extra={"table": sf_table, "diff": diff})

        results[sf_table] = {"pg": pg_count, "sf": sf_count, "diff": diff, "in_sync": in_sync and ok}
    return results


def run_in_background() -> None:
    """Called by main.py at startup. Loops forever, checking every
    SYNC_CHECK_INTERVAL_H hours. Runs in a daemon thread — never blocks shutdown."""
    log.info("Sync-check thread sleeping before first run", extra={"initial_delay_s": _INITIAL_DELAY_S})
    time.sleep(_INITIAL_DELAY_S)
    while True:
        try:
            results = run_sync_check()
            all_ok = all(v["in_sync"] for v in results.values())
            log.info("Sync check complete", extra={"results": results, "all_in_sync": all_ok})
        except Exception as exc:
            log.error("Sync check error", extra={"error": str(exc)}, exc_info=True)
        time.sleep(int(SYNC_CHECK_INTERVAL_H * 3600))


if __name__ == "__main__":
    import sys
    from .monitoring import configure_logging
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))

    results = run_sync_check()
    all_ok = all(v["in_sync"] for v in results.values())
    for table, r in results.items():
        status = "OK" if r["in_sync"] else "DRIFT"
        print(f"[{status}] {table}: PG={r['pg']:,}  SF={r['sf']:,}  diff={r['diff']:,}")
    sys.exit(0 if all_ok else 1)
