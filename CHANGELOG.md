# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-08-21

Initial public release.

### Added
- Core CDC pipeline: PostgreSQL logical replication (`pgoutput`) → Snowflake,
  via a SQL-polled replication slot (`src/pg_replication.py`,
  `src/pgoutput_parser.py`)
- Batched writes to Snowflake — `MERGE` upserts to a current-state table plus
  append-only `_HISTORY` tables, with same-batch dedup by primary key
  (`src/snowflake_sink.py`)
- Partitioned-table support via `publish_via_partition_root` and JSONB →
  `VARIANT` column mapping
- Crash-safe restart behavior: replays from the replication slot, retries
  Snowflake writes with exponential back-off
- Structured JSON logging, OpenTelemetry tracing, and optional agentless
  Datadog metrics/log shipping (`src/monitoring.py`)
- Liveness/readiness HTTP endpoints for container orchestrators
  (`src/health.py`)
- Background + standalone PostgreSQL ↔ Snowflake row-count reconciliation
  (`src/sync_check.py`)
- Operational scripts: resumable backfill, delta reconciliation,
  connectivity check, replication-prerequisite check
- Example schema (`customers` / `customer_devices`), `sql/` setup scripts,
  `Dockerfile`, and CI workflow
