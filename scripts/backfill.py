"""
scripts/backfill.py
────────────────────
One-time script to load existing PostgreSQL rows into the Snowflake
current-state tables (CUSTOMERS and CUSTOMER_DEVICES) before the CDC
service starts streaming.

The CDC service only captures changes from when the replication slot is
created onwards — it does NOT backfill pre-existing rows. Run this script
ONCE before starting the CDC service.

What this script does:
  1. Pages through public.customers        → CUSTOMERS (INSERT)
  2. Pages through public.customer_devices  → CUSTOMER_DEVICES (INSERT)
  History tables are NOT populated here — CDC handles history from go-live.

Resumability:
  Progress is saved to backfill_checkpoint.json in the working directory.
  Re-running the script resumes from the last saved page.

Usage:
  export PG_HOST=localhost
  export PG_DATABASE=appdb
  export PG_USER=cdc_reader
  export PG_PASSWORD=...            # or the SP_TENANT_ID/... cert vars for prod
  export SNOWFLAKE_ACCOUNT=...
  export SNOWFLAKE_USER=...
  export SNOWFLAKE_PASSWORD=...
  export SNOWFLAKE_DATABASE=ANALYTICS_DEV
  export SNOWFLAKE_SCHEMA=PUBLIC
  export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
  python scripts/backfill.py
"""

import json
import logging
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras
import snowflake.connector

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.pg_replication import _get_pg_password  # reuses the same auth logic as the live service

# ── Logging ──────────────────────────────────────────────────────────────── #
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backfill")

# ── Config ───────────────────────────────────────────────────────────────── #
PG_HOST     = os.environ["PG_HOST"]
PG_DATABASE = os.environ["PG_DATABASE"]
PG_USER     = os.environ["PG_USER"]
PG_PORT     = int(os.environ.get("PG_PORT", "5432"))

SF_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE  = os.environ.get("SNOWFLAKE_DATABASE",  "ANALYTICS_DEV")
SF_SCHEMA    = os.environ.get("SNOWFLAKE_SCHEMA",    "PUBLIC")

PAGE_SIZE  = int(os.environ.get("BACKFILL_PAGE_SIZE", "10000"))
CHECKPOINT = os.environ.get("BACKFILL_CHECKPOINT", "backfill_checkpoint.json")
INSERT_BATCH = int(os.environ.get("BACKFILL_INSERT_BATCH", "500"))

# JSONB columns arrive as Python dicts (psycopg2 auto-parses them). The
# Snowflake connector cannot bind dict/list directly — serialize to a JSON string.
_JSONB_COLS: frozenset = frozenset({"metadata", "tags"})

# Tables to backfill, in order: (pg_table, sf_table, pk_col_for_paging)
_TABLES = [
    ("customers", "CUSTOMERS", "id"),
    ("customer_devices", "CUSTOMER_DEVICES", "id"),
]


# ── Column conversion ────────────────────────────────────────────────────── #

def _pg_col_to_sf(name: str) -> str:
    """PG camelCase → Snowflake UPPER_SNAKE_CASE."""
    return re.sub(r'([a-z])([A-Z])', r'\1_\2', name).upper()


def _row_to_sf(row: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for pg_key, val in row.items():
        sf_key = _pg_col_to_sf(pg_key)
        if isinstance(val, (dict, list)):
            out[sf_key] = json.dumps(val)
        else:
            out[sf_key] = val
    return out


_VARIANT_SF_COLS: frozenset = frozenset(_pg_col_to_sf(c) for c in _JSONB_COLS)


# ── Connections ──────────────────────────────────────────────────────────── #

def _pg_connect() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER,
        password=_get_pg_password(), sslmode="require", connect_timeout=15,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn


def _sf_connect() -> snowflake.connector.SnowflakeConnection:
    if os.environ.get("SNOWFLAKE_ACCOUNT"):
        creds = {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "password": os.environ["SNOWFLAKE_PASSWORD"],
        }
    else:
        from azure.identity import ManagedIdentityCredential
        from azure.keyvault.secrets import SecretClient
        kv_mi, kv_name = os.environ["SNOWFLAKE_KV_MI_CLIENT_ID"], os.environ["SNOWFLAKE_KV_NAME"]
        mi = ManagedIdentityCredential(client_id=kv_mi)
        kv = SecretClient(vault_url=f"https://{kv_name}.vault.azure.net", credential=mi)
        creds = {
            "account": kv.get_secret("SnowflakeETLAccount").value,
            "user": kv.get_secret("SnowflakeETLLogin").value,
            "password": kv.get_secret("SnowflakeETLPassword").value,
        }
    return snowflake.connector.connect(
        account=creds["account"], user=creds["user"], password=creds["password"],
        warehouse=SF_WAREHOUSE, database=SF_DATABASE, schema=SF_SCHEMA,
        network_timeout=30, login_timeout=30,
    )


# ── Checkpoint ───────────────────────────────────────────────────────────── #

def _load_checkpoint() -> Dict[str, Any]:
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT) as f:
            cp = json.load(f)
        log.info("Resuming from checkpoint: %s", cp)
        return cp
    return {f"{t}_last_id": 0 for t, _, _ in _TABLES} | {f"{t}_done": False for t, _, _ in _TABLES}


def _save_checkpoint(cp: Dict[str, Any]) -> None:
    with open(CHECKPOINT, "w") as f:
        json.dump(cp, f, indent=2)


# ── SQL builder ──────────────────────────────────────────────────────────── #

def _build_page_insert_sql(sf_table: str, all_cols: List[str], n_rows: int) -> str:
    """
    Build a single multi-row INSERT for a batch of n_rows.
    PARSE_JSON must be applied in the SELECT, not the VALUES clause —
    Snowflake doesn't allow function calls inside VALUES.
    """
    cols_clause = ", ".join(all_cols) + ", _LOADED_AT"
    select_parts = [
        f"PARSE_JSON(${i})" if c in _VARIANT_SF_COLS else f"${i}"
        for i, c in enumerate(all_cols, start=1)
    ] + ["CURRENT_TIMESTAMP()"]
    select_clause = ", ".join(select_parts)
    row_tpl = "(" + ", ".join("%s" for _ in all_cols) + ")"
    values_clause = ", ".join(row_tpl for _ in range(n_rows))
    return (
        f"INSERT INTO {SF_DATABASE}.{SF_SCHEMA}.{sf_table} ({cols_clause}) "
        f"SELECT {select_clause} FROM VALUES {values_clause}"
    )


# ── Per-table backfill ───────────────────────────────────────────────────── #

def _backfill_table(pg_conn, sf_conn, pg_table: str, sf_table: str, start_id: int, cb=None) -> int:
    """Page through pg_table (keyset pagination on id), writing each page to
    sf_table with multi-row INSERT batches.

    On a fresh run (start_id == 0) the target table is truncated first, so
    re-runs are safe. On checkpoint resume (start_id > 0) rows with
    id > start_id are appended — no duplicates possible."""
    pg_cur, sf_cur = pg_conn.cursor(), sf_conn.cursor()
    sql_cache: Dict[int, str] = {}
    total, last_id = 0, start_id
    t_start = time.monotonic()

    if start_id == 0:
        log.info("[%s] Truncating table before fresh load…", sf_table)
        sf_cur.execute(f"TRUNCATE TABLE {SF_DATABASE}.{SF_SCHEMA}.{sf_table}")

    while True:
        pg_cur.execute(
            f"SELECT * FROM public.{pg_table} WHERE id > %s ORDER BY id LIMIT %s",
            (last_id, PAGE_SIZE),
        )
        rows = pg_cur.fetchall()
        if not rows:
            break

        sf_rows = [_row_to_sf(dict(r)) for r in rows]
        all_cols = list(sf_rows[0].keys())

        for batch_start in range(0, len(sf_rows), INSERT_BATCH):
            batch = sf_rows[batch_start:batch_start + INSERT_BATCH]
            n = len(batch)
            if n not in sql_cache:
                sql_cache[n] = _build_page_insert_sql(sf_table, all_cols, n)
            params = [val for row in batch for val in (row.get(c) for c in all_cols)]
            sf_cur.execute(sql_cache[n], params)

        last_id = int(rows[-1]["id"])
        total += len(rows)
        elapsed = time.monotonic() - t_start
        rate = total / elapsed if elapsed > 0 else 0
        log.info("[%s] %d rows written  last_id=%d  %.0f rows/s", sf_table, total, last_id, rate)
        if cb:
            cb(last_id)

    pg_cur.close()
    sf_cur.close()
    log.info("[%s] Backfill complete — %d total rows", sf_table, total)
    return last_id


# ── Entry point ──────────────────────────────────────────────────────────── #

def main() -> None:
    log.info("=" * 60)
    log.info("Backfill starting  PG=%s/%s  SF=%s.%s (warehouse=%s)  page=%d rows",
              PG_HOST, PG_DATABASE, SF_DATABASE, SF_SCHEMA, SF_WAREHOUSE, PAGE_SIZE)
    log.info("=" * 60)

    cp = _load_checkpoint()
    pg_conn, sf_conn = _pg_connect(), _sf_connect()
    log.info("Connected to PostgreSQL and Snowflake")

    try:
        for pg_table, sf_table, _pk in _TABLES:
            done_key, id_key = f"{pg_table}_done", f"{pg_table}_last_id"
            if cp.get(done_key):
                log.info("%s already done (checkpoint), skipping", pg_table)
                continue

            log.info("Backfilling %s → %s …", pg_table, sf_table)

            def _cb(last_id: int, id_key=id_key) -> None:
                cp[id_key] = last_id
                _save_checkpoint(cp)

            last_id = _backfill_table(pg_conn, sf_conn, pg_table, sf_table, cp.get(id_key, 0), _cb)
            cp[id_key], cp[done_key] = last_id, True
            _save_checkpoint(cp)
    finally:
        pg_conn.close()
        sf_conn.close()

    log.info("=" * 60)
    log.info("Backfill complete. Next: start the CDC service so it captures ongoing changes.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
