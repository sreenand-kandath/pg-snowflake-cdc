# pg-snowflake-cdc

A lightweight change-data-capture (CDC) service that streams row-level
changes from PostgreSQL to Snowflake in near real time, using PostgreSQL's
built-in logical replication — no Debezium, no Kafka, no extra infrastructure.

For each replicated table it maintains two things in Snowflake:

- **A current-state table**, kept in sync via `MERGE` (upsert/delete)
- **A `_HISTORY` table**, an append-only audit trail of every insert, update,
  and delete, with the PostgreSQL WAL LSN and commit timestamp attached

```
PostgreSQL (logical replication slot)
        │  pgoutput binary protocol
        ▼
  pgoutput_parser.py   → decodes WAL messages into ChangeEvent objects
        │
        ▼
  snowflake_sink.py    → buffers events, batches MERGE + INSERT per table
        │
        ▼
      Snowflake  (CUSTOMERS + CUSTOMERS_HISTORY, ...)
```

## Why this exists

Debezium + Kafka Connect is the "standard" way to do CDC, but it's a lot of
moving parts for a small number of tables. This project reads WAL changes by
directly polling the replication slot over a normal SQL connection
(`pg_logical_slot_get_binary_changes`) and writes straight to Snowflake — one
small Python service, no message broker required.

Trade-offs vs. Debezium: no multi-consumer fan-out, no schema registry, and
you write a bit more table-specific mapping code yourself (see
`_TABLE_MAP` in `src/snowflake_sink.py`). In exchange, the whole thing is a
few hundred lines of Python you can actually read end-to-end.

## Design notes worth knowing before you read the code

- **GET, not PEEK.** `poll_changes()` uses
  `pg_logical_slot_get_binary_changes()` (consume) instead of peek.
  `pg_replication_slot_advance()` only moves `confirmed_flush_lsn`, not
  `restart_lsn` — so a peek-then-advance design keeps restarting from the
  same WAL position forever. GET advances `restart_lsn` immediately, at the
  cost of a small window where events have left the slot before Snowflake
  has durably written them (see `snowflake_sink.py`'s retry/backoff — a
  batch that exhausts retries raises instead of being silently dropped, so
  a crash-and-restart is safe but a `MAX_RETRIES` exhaustion is not).
- **SQL polling, not the streaming replication protocol.** Azure Database
  for PostgreSQL Flexible Server (and some other managed Postgres) doesn't
  support Azure AD token auth over the replication-protocol connection —
  only over a normal SQL connection. Polling `pg_logical_slot_get_binary_changes`
  sidesteps that. If you're not on Azure, psycopg2's native
  `ReplicationCursor` streaming works too and avoids polling.
- **`publish_via_partition_root = true` matters if your table is
  partitioned.** Without it, pgoutput emits WAL messages under each
  partition's own name (e.g. `customers_p3`) instead of the parent table,
  and the column mapping breaks. `normalize_pg_table()` in
  `snowflake_sink.py` also strips known partition prefixes as a safety net.
- **`REPLICA IDENTITY FULL` is required** for UPDATE/DELETE messages to
  include full row data — otherwise TOAST-ed columns (e.g. large JSONB
  fields) show up as `NULL` in the WAL message when unchanged.
- **The batch MERGE uses `QUALIFY ROW_NUMBER()`** to dedupe by primary key
  within a single batch. Without it, if the same row changes twice inside
  one flush interval, Snowflake's `MERGE` inserts a duplicate instead of
  updating twice.

## Quick start

```bash
git clone <this-repo-url>
cd pg-snowflake-cdc
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env           # then fill in your own values
```

Set up the source and destination once:

```bash
psql "$DATABASE_URL" -f sql/postgres_setup.sql
snowsql -f sql/snowflake_setup.sql   # or run it in the Snowflake UI
```

Check everything is reachable, then run the service:

```bash
python scripts/test_connectivity.py
python scripts/check_replication_setup.py
python scripts/backfill.py            # one-time: load existing rows
python -m src.main                    # starts streaming ongoing changes
```

## Example schema

The code ships wired up to a small illustrative schema — swap it for your
own by editing `_TABLE_MAP`, `_PK_COLS`, and `_JSONB_COLS` in
`src/snowflake_sink.py`, and the matching SQL in `sql/`.

| PostgreSQL table | Snowflake tables | Notes |
|---|---|---|
| `public.customers` | `CUSTOMERS`, `CUSTOMERS_HISTORY` | `PARTITION BY HASH(tenant_id)`; has JSONB columns `metadata`/`tags` |
| `public.customer_devices` | `CUSTOMER_DEVICES`, `CUSTOMER_DEVICES_HISTORY` | one-to-many child of `customers` |

## Configuration

All configuration is via environment variables — see `.env.example` for the
full list with defaults. The main ones:

| Variable | Purpose |
|---|---|
| `PG_HOST`, `PG_DATABASE`, `PG_USER`, `PG_PORT` | Source PostgreSQL connection |
| `PG_PASSWORD` | Local/dev auth — set this to skip the Azure AD cert flow entirely |
| `CDC_TABLES` | Comma-separated tables to replicate |
| `CDC_SLOT`, `CDC_PUBLICATION` | Replication slot / publication names |
| `SNOWFLAKE_ACCOUNT`, `_USER`, `_PASSWORD`, `_DATABASE`, `_SCHEMA`, `_WAREHOUSE` | Destination Snowflake connection (local/dev) |
| `DD_API_KEY`, `DD_SITE`, `DD_AGENT_HOST` | Optional Datadog metrics/logs/traces — the service runs fine with none of these set |

### Credentials in production

The included auth path (`src/pg_replication.py`, `src/snowflake_sink.py`)
fetches secrets from **Azure Key Vault via a Managed Identity** — that's
what this was built against. If you're on a different cloud, swap
`_load_pg_credential()` and `SnowflakeSink._fetch_creds()` for your own
secrets backend (AWS Secrets Manager, HashiCorp Vault, plain env vars from
your orchestrator's secret store, etc.) — those two functions are the only
cloud-specific code in the project.

## Running the tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

`tests/test_pgoutput_parser.py` builds raw pgoutput binary messages by hand
and exercises the WAL parser directly — no live database needed.

## Operational scripts

| Script | Purpose |
|---|---|
| `scripts/backfill.py` | One-time load of existing rows before the CDC service starts (resumable via a checkpoint file) |
| `scripts/reconcile.py` | Re-MERGE rows changed since a given timestamp — for patching drift or the backfill→slot handoff gap |
| `scripts/check_replication_setup.py` | Read-only diagnostic for the PostgreSQL prerequisites |
| `scripts/test_connectivity.py` | Smoke-test both database connections before starting the service |
| `python -m src.sync_check` | One-shot PG↔Snowflake row-count comparison; exit code 1 on drift, handy in a cron/CI gate |

## Deploying

`Dockerfile` builds a minimal, non-root container exposing a health server
on `:8080` (`/healthz` liveness, `/readyz` readiness) — point your
orchestrator's probes at those. `.github/workflows/ci.yml` runs the test
suite and a Docker build on every push; wire in your own deploy step
(`docker push` + whatever updates your container platform).

Run only **one replica at a time** — a replication slot can only be consumed
by one connection, so a second instance will just sit retrying rather than
double-processing anything.

## License

MIT — see [LICENSE](LICENSE).
