"""
snowflake_sink.py
Buffers ChangeEvents and writes them in batches to Snowflake.

Two writes per flush, per table:
  1. Current-state table  — MERGE on primary key (idempotent upsert / delete)
  2. History table        — INSERT always        (full audit trail)

LSN safety:
  flush() returns the highest LSN confirmed to Snowflake in that batch.
  main.py uses this value to know how far it's safe to let the PostgreSQL
  replication slot advance, so the slot only moves forward AFTER Snowflake
  confirms the write.

  If the service crashes mid-flush, the events are re-read from the slot on
  restart and re-applied. MERGE on the current-state table is idempotent.
  The history table can get duplicate rows in the worst case (same _CDC_LSN
  twice), which can be de-duplicated with
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _CDC_LSN ORDER BY _LOADED_AT) = 1
  if needed.

Credentials:
  Fetched at connection time from Azure Key Vault via a user-assigned
  Managed Identity in production (see README.md), or read directly from
  SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PASSWORD for local dev.

Example schema (see sql/snowflake_setup.sql):
  public.customers          → CUSTOMERS + CUSTOMERS_HISTORY
  public.customer_devices   → CUSTOMER_DEVICES + CUSTOMER_DEVICES_HISTORY
Swap _TABLE_MAP / _PK_COLS / _JSONB_COLS below for your own schema.
"""

import datetime
import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import snowflake.connector

from .monitoring import (
    count_event,
    count_snowflake_error,
    count_snowflake_flush,
    gauge_snowflake_flush_ms,
)
from .pgoutput_parser import ChangeEvent

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────── #
_SF_KV_MI_CLIENT_ID = os.environ.get("SNOWFLAKE_KV_MI_CLIENT_ID", "")
_SF_KV_NAME         = os.environ.get("SNOWFLAKE_KV_NAME", "")
_SF_WAREHOUSE       = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
_SF_DATABASE        = os.environ.get("SNOWFLAKE_DATABASE",  "ANALYTICS_DEV")
_SF_SCHEMA          = os.environ.get("SNOWFLAKE_SCHEMA",    "PUBLIC")

_BATCH_SIZE: int       = int(os.environ.get("SF_BATCH_SIZE",        "500"))
_FLUSH_INTERVAL: float = float(os.environ.get("SF_FLUSH_INTERVAL_S", "10"))
_MAX_RETRIES: int      = int(os.environ.get("SF_MAX_RETRIES",        "5"))
_RETRY_BASE_S: float   = float(os.environ.get("SF_RETRY_BASE_S",     "2"))

# PG table name (as delivered by pgoutput) → (current_state_sf_table, history_sf_table)
_TABLE_MAP: Dict[str, Tuple[str, str]] = {
    "customers":        ("CUSTOMERS",        "CUSTOMERS_HISTORY"),
    "customer_devices": ("CUSTOMER_DEVICES", "CUSTOMER_DEVICES_HISTORY"),
}

# Partition prefix → parent table name.
# `customers` is PARTITION BY HASH(tenant_id) in the example schema. Without
# publish_via_partition_root=true in the publication, pgoutput delivers events
# under the partition's own name (e.g. "customers_p3") rather than the parent.
# Normalise here so we don't have to enumerate every partition.
_PARTITION_PREFIXES: Dict[str, str] = {
    "customers_": "customers",
}


def normalize_pg_table(table: str) -> str:
    """Map a partition table name to its parent, or return the table unchanged."""
    for prefix, parent in _PARTITION_PREFIXES.items():
        if table.startswith(prefix):
            return parent
    return table

# Primary-key columns for each current-state table (MERGE ON clause)
_PK_COLS: Dict[str, List[str]] = {
    "CUSTOMERS":        ["ID", "TENANT_ID"],
    "CUSTOMER_DEVICES": ["ID"],
}

# PG column names whose values are JSONB text
_JSONB_COLS: frozenset = frozenset({"metadata", "tags"})
# Same columns in Snowflake UPPER_SNAKE_CASE (built lazily after _pg_col_to_sf is defined)
_JSONB_SF_COLS: frozenset = frozenset()  # populated after helper is defined

# pgoutput op codes → single-char CDC label stored in history tables
_OP_LABEL: Dict[str, str] = {"c": "I", "u": "U", "d": "D"}


# ── Helpers ───────────────────────────────────────────────────────────────── #

def _pg_col_to_sf(name: str) -> str:
    """Convert a PG camelCase column name to Snowflake UPPER_SNAKE_CASE.

    id           → ID
    tenantId     → TENANT_ID
    """
    return re.sub(r'([a-z])([A-Z])', r'\1_\2', name).upper()


def _row_to_sf(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a pgoutput row dict to a Snowflake-ready dict.

    Keys:   PG camelCase   → UPPER_SNAKE_CASE
    Values: JSONB columns  → JSON string (SQL uses PARSE_JSON(%s) for VARIANT binding)
            everything else → unchanged (the Snowflake connector handles casting)
    """
    out: Dict[str, Any] = {}
    for pg_key, val in row.items():
        sf_key = _pg_col_to_sf(pg_key)
        if pg_key in _JSONB_COLS:
            if val is None:
                out[sf_key] = None
            elif isinstance(val, (dict, list)):
                out[sf_key] = json.dumps(val)
            else:
                out[sf_key] = val  # already a JSON string
        else:
            out[sf_key] = val
    return out


# Populate after _pg_col_to_sf is defined
_JSONB_SF_COLS: frozenset = frozenset(_pg_col_to_sf(c) for c in _JSONB_COLS)  # type: ignore[assignment]


def _ts_ms_to_str(ts_ms: int) -> str:
    """Unix-milliseconds → ISO string for TIMESTAMP_NTZ binding."""
    dt = datetime.datetime.utcfromtimestamp(ts_ms / 1000)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


# ── SQL builders ──────────────────────────────────────────────────────────── #

def _build_merge_row_select(all_cols: List[str]) -> str:
    """Single SELECT row used inside the USING (...) source of a batch MERGE."""
    return ", ".join(
        f"PARSE_JSON(%s) AS {c}" if c in _JSONB_SF_COLS else f"%s AS {c}"
        for c in all_cols
    )


def _build_batch_merge_sql(
    sf_table: str, pk_cols: List[str], all_cols: List[str], n_rows: int
) -> str:
    """MERGE that processes n_rows in a single statement via a UNION ALL source.

    The UNION ALL source is wrapped in a QUALIFY ROW_NUMBER() dedup so that if
    the same PK appears multiple times in one batch (e.g. rapid successive
    updates to the same row), only the LAST occurrence is applied. Without
    this, Snowflake's MERGE inserts a new row for every unmatched source row,
    producing duplicates.
    """
    non_pk    = [c for c in all_cols if c not in pk_cols]
    row_sel   = _build_merge_row_select(all_cols)
    union_src = "\n  UNION ALL\n  SELECT ".join([row_sel] * n_rows)
    pk_part   = ", ".join(pk_cols)
    non_pk_order = pk_cols[0]   # any stable col; last row wins
    dedup_src = (
        f"SELECT * FROM (SELECT {union_src}) "
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {pk_part} ORDER BY {non_pk_order} DESC) = 1"
    )
    on_      = " AND ".join(f"t.{c} = s.{c}" for c in pk_cols)
    upd_set  = ", ".join(f"t.{c} = s.{c}" for c in non_pk)
    upd_set += ", t._LOADED_AT = CURRENT_TIMESTAMP()"
    ins_cols = ", ".join(all_cols) + ", _LOADED_AT"
    ins_vals = ", ".join(f"s.{c}" for c in all_cols) + ", CURRENT_TIMESTAMP()"
    return (
        f"MERGE INTO {_SF_DATABASE}.{_SF_SCHEMA}.{sf_table} AS t "
        f"USING ({dedup_src}) AS s "
        f"ON {on_} "
        f"WHEN MATCHED THEN UPDATE SET {upd_set} "
        f"WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})"
    )


def _build_history_row_select(all_cols: List[str]) -> str:
    """Return the SELECT clause for a single history row (used in UNION ALL batching)."""
    return ", ".join(
        "PARSE_JSON(%s)" if c in _JSONB_SF_COLS else "%s"
        for c in all_cols
    ) + ", CURRENT_TIMESTAMP()"


# ── Sink class ────────────────────────────────────────────────────────────── #

class SnowflakeSink:
    """
    Accumulates ChangeEvents in memory and flushes them to Snowflake in
    batches. Designed to run on the single CDC thread.
    """

    def __init__(self) -> None:
        self._buffer: List[ChangeEvent] = []
        self._last_flush: float = time.monotonic()
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None
        # Per-table SQL templates — built once from the first row's columns
        self._merge_sql_cache: Dict[Any, str] = {}
        self._hist_sql_cache: Dict[Any, str] = {}
        log.info(
            "SnowflakeSink initialised",
            extra={
                "warehouse": _SF_WAREHOUSE, "database": _SF_DATABASE,
                "schema": _SF_SCHEMA, "batch_size": _BATCH_SIZE,
                "flush_interval_s": _FLUSH_INTERVAL,
            },
        )

    # ── Public interface ─────────────────────────────────────────────────── #

    def publish(self, event: ChangeEvent) -> Optional[int]:
        """Buffer one event. Auto-flushes when the batch is full or the
        flush interval has elapsed.

        Returns the highest confirmed LSN if a flush occurred, else None.
        The caller should pass any non-None return to advance_slot().
        """
        if normalize_pg_table(event.table) not in _TABLE_MAP:
            log.debug("Skipping unmapped table", extra={"table": event.table})
            return None
        event.table = normalize_pg_table(event.table)

        self._buffer.append(event)

        elapsed = time.monotonic() - self._last_flush
        if len(self._buffer) >= _BATCH_SIZE or elapsed >= _FLUSH_INTERVAL:
            return self._flush()
        return None

    def flush(self) -> Optional[int]:
        """Force-flush all buffered events. Call on shutdown or reconnect.

        Returns the highest confirmed LSN, or None if the buffer was empty.
        """
        return self._flush()

    # ── Internal ─────────────────────────────────────────────────────────── #

    def _get_conn(self) -> snowflake.connector.SnowflakeConnection:
        if self._conn is None or self._conn.is_closed():
            creds = self._fetch_creds()
            log.info(
                "Connecting to Snowflake",
                extra={"account": creds["account"], "user": creds["user"],
                       "warehouse": _SF_WAREHOUSE},
            )
            self._conn = snowflake.connector.connect(
                account=creds["account"],
                user=creds["user"],
                password=creds["password"],   # never logged
                warehouse=_SF_WAREHOUSE,
                database=_SF_DATABASE,
                schema=_SF_SCHEMA,
                login_timeout=60,         # allow warehouse cold-start
                network_timeout=120,      # HTTP-level retry window
                socket_timeout=120,       # underlying TLS read timeout
                client_session_keep_alive=True,  # prevent idle session expiry
            )
            log.info("Snowflake connection established")
        return self._conn

    def _fetch_creds(self) -> Dict[str, str]:
        """Prefer plain env vars (local dev); fall back to Key Vault (production)."""
        if os.environ.get("SNOWFLAKE_ACCOUNT"):
            return {
                "account": os.environ["SNOWFLAKE_ACCOUNT"],
                "user": os.environ["SNOWFLAKE_USER"],
                "password": os.environ["SNOWFLAKE_PASSWORD"],
            }

        from azure.identity import ManagedIdentityCredential
        from azure.keyvault.secrets import SecretClient

        log.info(
            "Fetching Snowflake credentials from Key Vault",
            extra={"kv": _SF_KV_NAME},
        )
        mi = ManagedIdentityCredential(client_id=_SF_KV_MI_CLIENT_ID)
        kv = SecretClient(vault_url=f"https://{_SF_KV_NAME}.vault.azure.net", credential=mi)
        return {
            "account":  kv.get_secret("SnowflakeETLAccount").value,
            "user":     kv.get_secret("SnowflakeETLLogin").value,
            "password": kv.get_secret("SnowflakeETLPassword").value,
        }

    def _flush(self) -> Optional[int]:
        if not self._buffer:
            self._last_flush = time.monotonic()
            return None

        batch = self._buffer[:]
        t0    = time.monotonic()
        log.info(
            "Flushing Snowflake batch",
            extra={"batch_size": len(batch),
                   "lsn_range": f"{batch[0].lsn}–{batch[-1].lsn}"},
        )

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                self._write_batch(batch)
                break
            except Exception as exc:
                if attempt < _MAX_RETRIES:
                    wait = min(_RETRY_BASE_S * (2 ** (attempt - 1)), 60.0)
                    log.warning(
                        "Snowflake write failed — will retry",
                        extra={"attempt": attempt, "wait_s": wait, "error": str(exc)},
                    )
                    time.sleep(wait)
                    self._conn = None   # force a fresh connection on next attempt
                else:
                    # All retries exhausted — raise so the caller never advances the
                    # PG slot LSN and events are re-delivered on the next restart.
                    log.error(
                        "Snowflake write failed after all retries — slot LSN held back",
                        extra={"error": str(exc), "batch_size": len(batch),
                               "first_lsn": batch[0].lsn, "last_lsn": batch[-1].lsn},
                    )
                    count_snowflake_error("batch", str(exc)[:64])
                    raise

        elapsed_ms = (time.monotonic() - t0) * 1000
        # Use the transaction COMMIT LSN (not the row-change LSN) so the slot
        # advances at a transaction boundary and the next poll starts AFTER
        # the fully-consumed transaction.
        max_lsn = max((e.commit_lsn if e.commit_lsn else e.lsn) for e in batch)

        self._buffer.clear()
        self._last_flush = time.monotonic()
        gauge_snowflake_flush_ms(elapsed_ms)
        log.info(
            "Snowflake flush complete",
            extra={"batch_size": len(batch), "elapsed_ms": round(elapsed_ms, 1),
                   "confirmed_lsn": max_lsn},
        )
        return max_lsn

    def _write_batch(self, events: List[ChangeEvent]) -> None:
        """Write a batch to Snowflake: group by table, MERGE current + INSERT history."""
        conn = self._get_conn()
        cur  = conn.cursor()
        try:
            by_table: Dict[str, List[ChangeEvent]] = {}
            for e in events:
                by_table.setdefault(normalize_pg_table(e.table), []).append(e)

            for pg_table, tbl_events in by_table.items():
                sf_current, sf_history = _TABLE_MAP[pg_table]
                pk_cols = _PK_COLS[sf_current]

                upserts = [e for e in tbl_events if e.op in ("c", "u")]
                deletes = [e for e in tbl_events if e.op == "d"]

                if upserts:
                    self._merge_current(cur, sf_current, pk_cols, upserts)
                if deletes:
                    self._delete_current(cur, sf_current, pk_cols, deletes)

                self._insert_history(cur, sf_history, tbl_events)

                count_snowflake_flush(pg_table, len(tbl_events))
                count_event(pg_table, "flushed", stage="published")
        finally:
            cur.close()

    def _merge_current(self, cur, sf_table: str, pk_cols: List[str], events: List[ChangeEvent]) -> None:
        rows = [_row_to_sf(e.after or {}) for e in events if e.after]
        if not rows:
            return

        all_cols = list(rows[0].keys())
        sql = _build_batch_merge_sql(sf_table, pk_cols, all_cols, len(rows))
        flat_params = [v for row in rows for v in [row.get(c) for c in all_cols]]
        cur.execute(sql, flat_params)

        # Logged at INFO so you can search your log platform by table+PK to
        # trace a specific record from the Postgres change through to the
        # Snowflake MERGE.
        sample_pks = [{c: row.get(c) for c in pk_cols} for row in rows[:10]]
        log.info("MERGE executed", extra={"table": sf_table, "rows": len(rows), "pk_sample": sample_pks})

    def _delete_current(self, cur, sf_table: str, pk_cols: List[str], events: List[ChangeEvent]) -> None:
        where = " AND ".join(f"{c} = %s" for c in pk_cols)
        sql   = f"DELETE FROM {_SF_DATABASE}.{_SF_SCHEMA}.{sf_table} WHERE {where}"
        for e in events:
            row = _row_to_sf(e.before or {})
            cur.execute(sql, [row.get(c) for c in pk_cols])
        log.debug("DELETE executed", extra={"table": sf_table, "rows": len(events)})

    def _insert_history(self, cur, sf_hist_table: str, events: List[ChangeEvent]) -> None:
        rows: List[Dict[str, Any]] = []
        for e in events:
            # For insert/update use the after-state; for delete use the
            # before-state so the deleted row's last values are preserved
            # in the audit trail.
            data = e.after if e.op in ("c", "u") else e.before
            if data is None:
                continue
            row = _row_to_sf(data)
            row["_CDC_OP"]  = _OP_LABEL.get(e.op, e.op)
            row["_CDC_LSN"] = e.lsn
            row["_CDC_TS"]  = _ts_ms_to_str(e.ts_ms)
            rows.append(row)

        if not rows:
            return

        all_cols   = list(rows[0].keys())
        col_list   = ", ".join(all_cols) + ", _LOADED_AT"
        row_select = _build_history_row_select(all_cols)
        union_body = "\nUNION ALL\nSELECT ".join([row_select] * len(rows))
        sql = f"INSERT INTO {_SF_DATABASE}.{_SF_SCHEMA}.{sf_hist_table} ({col_list}) SELECT {union_body}"
        flat_params = [v for r in rows for v in [r.get(c) for c in all_cols]]
        cur.execute(sql, flat_params)

        log.debug("INSERT history executed", extra={"table": sf_hist_table, "rows": len(rows)})
