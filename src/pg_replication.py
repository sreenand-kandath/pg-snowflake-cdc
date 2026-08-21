"""
pg_replication.py
Manages the PostgreSQL logical replication connection and slot.

Auth flow (production):
  1. A user-assigned Managed Identity fetches a client certificate (PFX) from
     Azure Key Vault.
  2. CertificateCredential uses that certificate to obtain Azure AD tokens
     (caching + auto-refresh handled by the azure-identity SDK).
  3. The fresh token is used as the PostgreSQL password on every (re)connect
     — Azure Database for PostgreSQL Flexible Server supports AAD token auth
     in place of a static password.

  Local development: set PG_PASSWORD directly and skip all of the above
  (see the "Local dev" section in README.md).

This module is Azure-specific by default, but the only Azure-shaped code is
`_load_pg_credential()` below — swap it out for AWS IAM auth, HashiCorp Vault,
or a plain password if you're running against a different Postgres host.
"""

import base64
import logging
import os
import select
import tempfile
import time
from contextlib import contextmanager
from typing import Iterator

import psycopg2
import psycopg2.extras

from .monitoring import gauge_lag

log = logging.getLogger(__name__)

# ── env-var config ───────────────────────────────────────────────────────── #
PG_HOST = os.environ["PG_HOST"]
PG_DATABASE = os.environ["PG_DATABASE"]
PG_USER = os.environ["PG_USER"]
PG_PORT = int(os.environ.get("PG_PORT", "5432"))

# Azure AD certificate auth — only required when PG_PASSWORD is not set.
SP_TENANT_ID = os.environ.get("SP_TENANT_ID", "")
SP_CLIENT_ID = os.environ.get("SP_CLIENT_ID", "")
SOURCE_KV_MI_CLIENT_ID = os.environ.get("SOURCE_KV_MI_CLIENT_ID", "")
SOURCE_KV_NAME = os.environ.get("SOURCE_KV_NAME", "")
CERT_SECRET_NAME = os.environ.get("CERT_SECRET_NAME", "cert-cdc-reader")

PUBLICATION_NAME = os.environ.get("CDC_PUBLICATION", "cdc_pub")
SLOT_NAME = os.environ.get("CDC_SLOT", "cdc_slot")

_PG_TOKEN_RESOURCE = "https://ossrdbms-aad.database.windows.net/.default"

# ── Certificate bootstrap (production auth path) ────────────────────────── #


def _load_pg_credential():
    """
    Fetch the PFX certificate from Key Vault using the user-assigned Managed
    Identity, write it to a secure temp file, and return a CertificateCredential.
    The temp file persists for the process lifetime (the credential needs it).
    """
    from azure.identity import CertificateCredential, ManagedIdentityCredential
    from azure.keyvault.secrets import SecretClient

    log.info(
        "Fetching SP certificate from Key Vault",
        extra={"kv": SOURCE_KV_NAME, "secret": CERT_SECRET_NAME},
    )
    mi = ManagedIdentityCredential(client_id=SOURCE_KV_MI_CLIENT_ID)
    kv = SecretClient(vault_url=f"https://{SOURCE_KV_NAME}.vault.azure.net", credential=mi)
    pfx_bytes = base64.b64decode(kv.get_secret(CERT_SECRET_NAME).value)

    # Write to /dev/shm (in-memory) when available, else the OS temp dir.
    cert_dir = "/dev/shm" if os.path.isdir("/dev/shm") else tempfile.gettempdir()
    cert_path = os.path.join(cert_dir, "pg-cdc-cert.pfx")
    with open(cert_path, "wb") as f:
        f.write(pfx_bytes)
    os.chmod(cert_path, 0o600)
    log.info("Certificate written to secure path", extra={"path": cert_path})

    return CertificateCredential(
        tenant_id=SP_TENANT_ID,
        client_id=SP_CLIENT_ID,
        certificate_path=cert_path,
        password=None,  # Key Vault exports are passwordless
    )


_pg_credential = None


def _get_pg_password() -> str:
    """Return the password to use for the next PostgreSQL connection.

    Prefers PG_PASSWORD (local/dev) over the Azure AD certificate flow
    (production), so the same code path works in both environments.
    """
    global _pg_credential

    if os.environ.get("PG_PASSWORD"):
        return os.environ["PG_PASSWORD"]

    if _pg_credential is None:
        _pg_credential = _load_pg_credential()
    token = _pg_credential.get_token(_PG_TOKEN_RESOURCE)
    log.debug("PostgreSQL AAD token acquired", extra={"expires_on": token.expires_on})
    return token.token


# ── Connection helpers ───────────────────────────────────────────────────── #


def ensure_publication(tables: list[str]) -> None:
    """
    Ensure the publication exists for the given tables. Idempotent — safe to
    call on every startup. Uses a normal (non-replication) connection so DDL
    is allowed.
    """
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
        user=PG_USER, password=_get_pg_password(), sslmode="require",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_publication WHERE pubname = %s", (PUBLICATION_NAME,)
            )
            if cur.fetchone():
                log.info("Publication exists — ensuring publish_via_partition_root=true",
                         extra={"publication": PUBLICATION_NAME})
                try:
                    cur.execute(
                        f"ALTER PUBLICATION {PUBLICATION_NAME} SET (publish_via_partition_root = true)"
                    )
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    log.warning(
                        "Could not ALTER publication (run as admin if needed)",
                        extra={"publication": PUBLICATION_NAME, "error": str(exc)},
                    )
                return
            table_list = ", ".join(tables)
            cur.execute(
                f"CREATE PUBLICATION {PUBLICATION_NAME} FOR TABLE {table_list}"
                f" WITH (publish_via_partition_root = true)"
            )
            conn.commit()
            log.info("Publication created",
                     extra={"publication": PUBLICATION_NAME, "tables": tables})
    finally:
        conn.close()


# ── SQL polling approach (no replication-protocol connection required) ───── #
# Uses pg_logical_slot_get_binary_changes() over a regular SQL connection so
# that Azure AD token authentication works — the replication-protocol
# connection path does not support AAD token auth on Azure PG Flexible Server.
# If you're not on Azure and don't need AAD auth, psycopg2's native
# ReplicationCursor / START_REPLICATION streaming works too and avoids polling.

POLL_INTERVAL_S: float = float(os.environ.get("CDC_POLL_INTERVAL_S", "2.0"))
POLL_BATCH_SIZE: int = int(os.environ.get("CDC_POLL_BATCH_SIZE", "500"))


def _lsn_str_to_int(lsn_str: str) -> int:
    """Convert '1A/2B3C4D5E' → integer."""
    hi, lo = lsn_str.split("/")
    return (int(hi, 16) << 32) | int(lo, 16)


def _lsn_int_to_str(lsn: int) -> str:
    """Convert integer → '1A/2B3C4D5E'."""
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:08X}"


def _ensure_slot_sql(conn: psycopg2.extensions.connection) -> None:
    """Create the replication slot via a regular SQL connection if not present."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s",
            (SLOT_NAME,),
        )
        if cur.fetchone():
            log.info("Replication slot exists", extra={"slot": SLOT_NAME})
        else:
            log.info("Creating replication slot", extra={"slot": SLOT_NAME, "plugin": "pgoutput"})
            cur.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                (SLOT_NAME,),
            )


def _release_stale_slot_backend(conn: psycopg2.extensions.connection) -> None:
    """
    If the replication slot is held by another backend (e.g. the previous
    container revision during a rolling deploy), terminate that backend so
    this instance can acquire the slot immediately.

    Requires pg_signal_backend privilege on the CDC user — see sql/postgres_setup.sql.
    If the privilege is absent the terminate call just logs a warning and the
    normal retry/back-off loop in main.py handles it.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s",
            (SLOT_NAME,),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return  # slot not active — nothing to do
        pid = row[0]
        log.warning("Replication slot held by another backend — terminating it",
                    extra={"slot": SLOT_NAME, "stale_pid": pid})
        try:
            cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
            terminated = cur.fetchone()[0]
            if terminated:
                log.info("Stale backend terminated — slot now free", extra={"pid": pid})
            else:
                log.warning("pg_terminate_backend returned false — slot should free shortly",
                            extra={"pid": pid})
        except Exception as exc:  # no pg_signal_backend privilege
            log.warning(
                "Cannot terminate stale backend (grant pg_signal_backend to the CDC "
                "user — see sql/postgres_setup.sql)",
                extra={"error": str(exc)},
            )


@contextmanager
def cdc_connection() -> Iterator[psycopg2.extensions.connection]:
    """
    Context manager that opens a regular SQL connection, ensures the
    replication slot exists, and yields the connection.
    """
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DATABASE,
        user=PG_USER, password=_get_pg_password(),
        sslmode="require", connect_timeout=15,
    )
    conn.autocommit = True
    try:
        _release_stale_slot_backend(conn)
        _ensure_slot_sql(conn)
        log.info("CDC connection ready — polling with SQL slot functions")
        yield conn
    finally:
        try:
            conn.close()
        except Exception:
            pass
        log.info("CDC SQL connection closed")


def poll_changes(
    conn: psycopg2.extensions.connection,
    batch_size: int = POLL_BATCH_SIZE,
) -> list:
    """
    Consume pending WAL changes and advance the slot's restart position in
    one atomic step. Returns a list of (lsn_int, payload_bytes).

    Uses pg_logical_slot_get_binary_changes() (CONSUME) rather than PEEK, so
    that restart_lsn moves forward immediately. PEEK + pg_replication_slot_advance()
    looks like it works (confirmed_flush_lsn updates) but restart_lsn never
    moves, so the same events get returned on every subsequent peek.

    Trade-off: events are removed from the slot before the downstream write
    completes. If a flush fails after all retries, main.py must not advance
    past those events, and they're logged so they aren't silently lost.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT lsn, data
            FROM pg_logical_slot_get_binary_changes(
                %s, NULL, %s,
                'proto_version', '1',
                'publication_names', %s
            )
            """,
            (SLOT_NAME, batch_size, PUBLICATION_NAME),
        )
        rows = cur.fetchall()

    result = []
    for lsn_val, data in rows:
        lsn_int = _lsn_str_to_int(lsn_val) if isinstance(lsn_val, str) else int(lsn_val)
        result.append((lsn_int, bytes(data)))
    return result


def advance_slot(conn: psycopg2.extensions.connection, lsn: int) -> None:
    """
    Advance the replication slot's confirmed LSN.
    Call ONLY after the downstream sink (Snowflake) has durably confirmed
    the write for all events up to and including lsn.
    """
    lsn_str = _lsn_int_to_str(lsn)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_replication_slot_advance(%s, %s)", (SLOT_NAME, lsn_str))
    log.debug("Slot advanced", extra={"slot": SLOT_NAME, "lsn": lsn_str})
