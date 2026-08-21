"""
scripts/reconcile.py
─────────────────────
Delta reconciliation: re-MERGEs any PostgreSQL rows updated since a given
timestamp into the Snowflake current-state tables.

Use this to patch a gap, for example:
  - Rows changed between when scripts/backfill.py started reading a table
    and when the CDC replication slot was actually created (backfill.py
    takes a plain snapshot; it doesn't guarantee you a precise handoff LSN).
  - After discovering drift via `python -m src.sync_check`.

This does NOT touch the history tables — it only fixes the current-state
tables, since history is meant to be an append-only CDC event log, not a
target for backfilled corrections.

Usage:
  python scripts/reconcile.py --since "2026-08-01T00:00:00Z"
  python scripts/reconcile.py --since "2026-08-01T00:00:00Z" --table customers
"""

import argparse
import logging
import os
import sys

import psycopg2
import psycopg2.extras

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.pg_replication import PG_HOST, PG_DATABASE, PG_USER, PG_PORT, _get_pg_password
from src.snowflake_sink import (
    SnowflakeSink,
    _TABLE_MAP,
    _PK_COLS,
    _build_batch_merge_sql,
    _row_to_sf,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("reconcile")

_BATCH = int(os.environ.get("RECONCILE_BATCH_SIZE", "500"))


def _pg_connect() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER,
        password=_get_pg_password(), sslmode="require", connect_timeout=15,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    conn.set_session(readonly=True, autocommit=True)
    return conn


def _reconcile_table(pg_conn, sink: SnowflakeSink, pg_table: str, since: str) -> int:
    sf_current, _sf_history = _TABLE_MAP[pg_table]
    pk_cols = _PK_COLS[sf_current]

    cur = pg_conn.cursor()
    cur.execute(
        f"SELECT * FROM public.{pg_table} WHERE updated_at >= %s ORDER BY id",  # noqa: S608
        (since,),
    )

    total = 0
    conn = sink._get_conn()  # reuse the sink's pooled Snowflake connection
    sf_cur = conn.cursor()
    try:
        while True:
            rows = cur.fetchmany(_BATCH)
            if not rows:
                break
            sf_rows = [_row_to_sf(dict(r)) for r in rows]
            all_cols = list(sf_rows[0].keys())
            sql = _build_batch_merge_sql(sf_current, pk_cols, all_cols, len(sf_rows))
            params = [v for row in sf_rows for v in (row.get(c) for c in all_cols)]
            sf_cur.execute(sql, params)
            total += len(rows)
            log.info("[%s] MERGEd %d rows so far", sf_current, total)
    finally:
        sf_cur.close()

    return total


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", required=True, help="ISO timestamp, e.g. 2026-08-01T00:00:00Z")
    parser.add_argument("--table", choices=list(_TABLE_MAP.keys()), help="Reconcile a single table only")
    args = parser.parse_args()

    tables = [args.table] if args.table else list(_TABLE_MAP.keys())

    pg_conn = _pg_connect()
    sink = SnowflakeSink()
    try:
        for pg_table in tables:
            log.info("Reconciling %s since %s …", pg_table, args.since)
            n = _reconcile_table(pg_conn, sink, pg_table, args.since)
            log.info("Reconciled %s: %d rows", pg_table, n)
    finally:
        pg_conn.close()


if __name__ == "__main__":
    main()
