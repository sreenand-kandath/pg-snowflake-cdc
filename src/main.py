"""
main.py
Entry point for the CDC service.

Runs a tight loop:
  1. Ensure the publication exists for the target tables.
  2. Poll the replication slot for new WAL changes.
  3. Parse pgoutput binary → ChangeEvent.
  4. Write events to Snowflake (MERGE current-state + INSERT history).
  5. On any error: exponential back-off, then reconnect.
     The replication slot preserves the LSN, so no events are lost across
     reconnects (network blips, token expiry, restarts, etc.).

Resilience notes:
  - SIGTERM is caught; in-flight Snowflake writes are flushed before exit.
  - Only ONE replica should run at a time — the replication slot can only be
    consumed by one connection, so a second instance will just wait/retry.
  - Back-off caps at BACKOFF_MAX seconds to prevent a thundering herd on
    reconnect storms.
  - The health server on :8080 (see health.py) exposes liveness + readiness
    probes for your container orchestrator.

Tables replicated (comma-separated env var CDC_TABLES):
  Default: public.customers,public.customer_devices
"""

import logging
import os
import signal
import time

from .health import record_event, set_ready, start_health_server
from .monitoring import configure_logging, count_event, count_reconnect, gauge_lag, gauge_poll_time, get_tracer, send_heartbeat
from .pg_replication import cdc_connection, ensure_publication, poll_changes, POLL_INTERVAL_S
from .pgoutput_parser import PgOutputParser
from .snowflake_sink import SnowflakeSink, _TABLE_MAP as _SINK_TABLES, normalize_pg_table

# ── Bootstrap ────────────────────────────────────────────────────────────── #
configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("cdc.main")
_tracer = get_tracer()

# ── Config ───────────────────────────────────────────────────────────────── #
_RAW_TABLES = os.environ.get("CDC_TABLES", "public.customers,public.customer_devices")
TABLES = [t.strip() for t in _RAW_TABLES.split(",") if t.strip()]

BACKOFF_BASE: int = int(os.environ.get("BACKOFF_BASE_S", "5"))
BACKOFF_MAX: int = int(os.environ.get("BACKOFF_MAX_S", "120"))

# ── Graceful shutdown ────────────────────────────────────────────────────── #
_shutdown = False

# Snowflake sink — single instance shared across reconnects so the buffer
# and SQL caches survive PostgreSQL reconnections.
_sink = SnowflakeSink()


def _handle_sigterm(signum, frame) -> None:  # noqa: ARG001
    global _shutdown
    log.info("SIGTERM received — initiating graceful shutdown", extra={"signal": signum})
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


# ── Main loop ────────────────────────────────────────────────────────────── #

def run() -> None:
    log.info("pg-snowflake-cdc starting",
             extra={"tables": TABLES, "backoff_base_s": BACKOFF_BASE, "backoff_max_s": BACKOFF_MAX})

    start_health_server()

    # Optional background reconciliation thread — compares PG vs Snowflake
    # row counts on a schedule and posts drift alerts. See sync_check.py.
    import threading as _threading
    from .sync_check import run_in_background as _sync_bg
    _threading.Thread(target=_sync_bg, daemon=True, name="sync-check").start()
    log.info("Background sync-check thread started",
             extra={"interval_h": float(os.environ.get("SYNC_CHECK_INTERVAL_H", "24"))})

    # Ensure the publication exists (idempotent). If this user lacks
    # CREATE PUBLICATION, run sql/postgres_setup.sql manually as an admin.
    try:
        log.info("Verifying PostgreSQL publication", extra={"tables": TABLES})
        ensure_publication(TABLES)
    except Exception as exc:
        log.warning("Could not verify/create publication — continuing with existing slot",
                    extra={"error": str(exc)})

    attempt = 0
    total_events = 0
    stream_start_ts = time.monotonic()

    while not _shutdown:
        try:
            attempt += 1
            if attempt > 1:
                count_reconnect()
            log.info("Opening replication stream", extra={"attempt": attempt, "total_events_so_far": total_events})

            parser = PgOutputParser()
            set_ready(False)

            with cdc_connection() as conn:
                attempt = 0
                set_ready(True)
                log.info("CDC polling active — reading WAL via SQL slot functions")
                stream_start_ts = time.monotonic()

                while not _shutdown:
                    _poll_start = time.monotonic()
                    changes = poll_changes(conn)
                    gauge_poll_time((time.monotonic() - _poll_start) * 1000)

                    if not changes:
                        send_heartbeat()
                        _sink.flush()
                        time.sleep(POLL_INTERVAL_S)
                        continue

                    with _tracer.start_as_current_span("cdc.poll_batch") as poll_span:
                        poll_span.set_attribute("cdc.batch_size", len(changes))

                        for lsn, payload in changes:
                            if _shutdown:
                                break

                            event = parser.parse(payload, lsn)
                            if event is None:
                                continue

                            if normalize_pg_table(event.table) not in _SINK_TABLES:
                                log.debug("Skipping unmapped table", extra={"table": event.table})
                                continue

                            with _tracer.start_as_current_span("cdc.process_event") as span:
                                span.set_attribute("db.operation", event.op)
                                span.set_attribute("db.sql.table", event.table)
                                span.set_attribute("cdc.lsn", str(lsn))
                                span.set_attribute("cdc.ts_ms", event.ts_ms)

                                log.debug("WAL event received", extra={
                                    "op": event.op, "schema": event.schema, "table": event.table,
                                    "lsn": lsn, "ts_ms": event.ts_ms,
                                })

                                _sink.publish(event)
                                record_event()
                                total_events += 1
                                count_event(normalize_pg_table(event.table), event.op)

                        if total_events % 1_000 == 0 and total_events > 0:
                            elapsed = time.monotonic() - stream_start_ts
                            rate = total_events / elapsed if elapsed > 0 else 0
                            log.info("Throughput checkpoint", extra={
                                "total_events": total_events, "elapsed_s": round(elapsed, 1),
                                "events_per_s": round(rate, 1),
                            })

                # Flush remaining buffered events before the connection closes.
                _sink.flush()

        except KeyboardInterrupt:
            log.info("KeyboardInterrupt — flushing and exiting")
            break

        except Exception as exc:
            set_ready(False, error=str(exc))
            err_str = str(exc)
            # Slot contention is transient during rolling restarts — the old
            # container holds the slot briefly. Warn and retry, don't alert.
            _slot_busy = "is active for PID" in err_str and "replication slot" in err_str
            if _slot_busy:
                log.warning("Replication slot busy — waiting for old connection to release",
                            extra={"attempt": attempt, "error": err_str})
            else:
                log.error("Replication stream error", exc_info=True,
                          extra={"attempt": attempt, "error": err_str})
            backoff = min(BACKOFF_BASE * (2 ** min(attempt - 1, 6)), BACKOFF_MAX)
            log.info("Back-off before reconnect", extra={"backoff_s": backoff, "attempt": attempt})
            time.sleep(backoff)

    log.info("Shutdown: flushing remaining Snowflake batch", extra={"total_events": total_events})
    _sink.flush()  # slot cannot be advanced here (connection closed); safe — re-merged on restart
    log.info("pg-snowflake-cdc stopped cleanly")


if __name__ == "__main__":
    run()
