"""
scripts/test_connectivity.py
──────────────────────────────
Quick smoke test for both ends of the pipeline. Run this before starting the
CDC service for the first time, or whenever you change credentials/network
config, to fail fast with a clear message instead of debugging inside the
main service loop.

Usage:
  python scripts/test_connectivity.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def check_postgres() -> bool:
    import psycopg2
    from src.pg_replication import PG_HOST, PG_DATABASE, PG_USER, PG_PORT, _get_pg_password

    print(f"→ PostgreSQL: connecting to {PG_HOST}:{PG_PORT}/{PG_DATABASE} as {PG_USER} …")
    try:
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE, user=PG_USER,
            password=_get_pg_password(), sslmode="require", connect_timeout=10,
        )
        cur = conn.cursor()
        cur.execute("SELECT version()")
        print("  connected:", cur.fetchone()[0].split(",")[0])

        cur.execute("SHOW wal_level")
        wal_level = cur.fetchone()[0]
        print(f"  wal_level = {wal_level}", "✓" if wal_level == "logical" else "✗ (must be 'logical')")

        cur.execute(
            "SELECT rolreplication FROM pg_roles WHERE rolname = current_user"
        )
        can_replicate = cur.fetchone()[0]
        print(f"  REPLICATION privilege = {can_replicate}", "✓" if can_replicate else "✗")

        conn.close()
        return wal_level == "logical" and can_replicate
    except Exception as exc:
        print("  FAILED:", exc)
        return False


def check_snowflake() -> bool:
    import snowflake.connector

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

    database = os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS_DEV")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
    warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

    print(f"→ Snowflake: connecting to {creds['account']} as {creds['user']} …")
    try:
        conn = snowflake.connector.connect(
            account=creds["account"], user=creds["user"], password=creds["password"],
            warehouse=warehouse, database=database, schema=schema,
            login_timeout=30, network_timeout=30,
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_VERSION()")
        print("  connected: Snowflake", cur.fetchone()[0])

        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
            (schema,),
        )
        tables = [r[0] for r in cur.fetchall()]
        print(f"  tables in {database}.{schema}:", tables or "(none — run sql/snowflake_setup.sql)")

        conn.close()
        return True
    except Exception as exc:
        print("  FAILED:", exc)
        return False


if __name__ == "__main__":
    pg_ok = check_postgres()
    print()
    sf_ok = check_snowflake()
    print()
    print("Result:", "ALL OK ✓" if (pg_ok and sf_ok) else "ISSUES FOUND ✗")
    sys.exit(0 if (pg_ok and sf_ok) else 1)
