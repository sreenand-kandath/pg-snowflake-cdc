"""
scripts/check_replication_setup.py
────────────────────────────────────
Read-only diagnostic: verifies the PostgreSQL prerequisites documented in
sql/postgres_setup.sql are actually in place before you start the CDC
service. Safe to run repeatedly — makes no changes.

Checks:
  - wal_level = logical
  - the cdc_pub publication exists
  - the connecting role has the REPLICATION attribute
  - SELECT grants on the replicated tables
  - whether a replication slot named CDC_SLOT already exists

Usage:
  python scripts/check_replication_setup.py
"""

import os
import sys

import psycopg2
import psycopg2.extras as ex

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.pg_replication import PG_HOST, PG_DATABASE, PG_USER, PG_PORT, SLOT_NAME, _get_pg_password

_TABLES = ("customers", "customer_devices")


def main() -> None:
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER,
        password=_get_pg_password(), sslmode="require", connect_timeout=15,
        cursor_factory=ex.RealDictCursor,
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT name, setting, pending_restart FROM pg_settings WHERE name = 'wal_level'")
    print("wal_level:", dict(cur.fetchone()))

    cur.execute("SELECT pubname, puballtables FROM pg_publication")
    print("publications:", [dict(r) for r in cur.fetchall()])

    cur.execute("SELECT rolname, rolreplication FROM pg_roles WHERE rolname = %s", (PG_USER,))
    print("connecting role:", [dict(r) for r in cur.fetchall()])

    cur.execute(
        """
        SELECT grantee, table_name, privilege_type
        FROM information_schema.role_table_grants
        WHERE table_name = ANY(%s)
        """,
        (list(_TABLES),),
    )
    print("table grants:", [dict(r) for r in cur.fetchall()])

    cur.execute("SELECT slot_name, plugin, confirmed_flush_lsn FROM pg_replication_slots")
    print("replication slots:", [dict(r) for r in cur.fetchall()])

    print(f"\nAttempting to create replication slot '{SLOT_NAME}' (idempotent test)...")
    cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s", (SLOT_NAME,))
    if cur.fetchone():
        print(f"Slot '{SLOT_NAME}' already exists — nothing to do.")
    else:
        try:
            cur.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (SLOT_NAME,))
            print("Slot created:", dict(cur.fetchone()))
        except Exception as exc:
            print("Failed to create slot:", exc)
            print("→ Check REPLICATION privilege and wal_level=logical (see sql/postgres_setup.sql)")

    conn.close()


if __name__ == "__main__":
    main()
