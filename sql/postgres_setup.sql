-- sql/postgres_setup.sql
-- Run once (as a superuser / admin role) on each PostgreSQL server before
-- starting the CDC service for the first time.
--
-- Replace "cdc_reader" below with whatever role the service actually
-- connects as.
-- ──────────────────────────────────────────────────────────────────────────

-- 1. Grant the connecting role REPLICATION privilege
--    (required to create and use a logical replication slot)
ALTER ROLE cdc_reader REPLICATION;

-- 2. Grant pg_signal_backend so the CDC service can terminate a stale
--    backend that's holding the replication slot during a rolling restart
--    (see _release_stale_slot_backend() in src/pg_replication.py).
--    Requires PostgreSQL 14+.
--    Verify with:
--      SELECT rolname, member::regrole FROM pg_auth_members m
--      JOIN pg_roles r ON r.oid = m.roleid WHERE r.rolname = 'pg_signal_backend';
GRANT pg_signal_backend TO cdc_reader;

-- 3. Grant read access on the tables being replicated
GRANT SELECT ON public.customers        TO cdc_reader;
GRANT SELECT ON public.customer_devices TO cdc_reader;

-- 4. Enable REPLICA IDENTITY FULL so that UPDATE and DELETE WAL records
--    always include all column values (not just the primary key). Without
--    this, TOAST-ed columns (e.g. large JSONB fields) appear as NULL in
--    pgoutput UPDATE messages.
ALTER TABLE public.customers        REPLICA IDENTITY FULL;
ALTER TABLE public.customer_devices REPLICA IDENTITY FULL;

-- 5. Create the publication.
--    publish_via_partition_root=true is REQUIRED if `customers` (or any
--    replicated table) uses declarative partitioning. Without it, pgoutput
--    sends a separate Relation message per partition and the parser in this
--    project loses the column mapping (see normalize_pg_table() in
--    src/snowflake_sink.py, which works around any leftover partition names).
CREATE PUBLICATION cdc_pub
  FOR TABLE public.customers, public.customer_devices
  WITH (publish_via_partition_root = true);

-- 6. The replication slot itself is created automatically by the CDC
--    service on first start — no manual action needed.

-- 7. Verify
SELECT slot_name, plugin, confirmed_flush_lsn
FROM   pg_replication_slots
WHERE  slot_name = 'cdc_slot';

SELECT pubname, puballtables
FROM   pg_publication
WHERE  pubname = 'cdc_pub';
