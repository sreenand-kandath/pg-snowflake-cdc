# Security Policy

## Supported versions

The latest tagged release (and `main`) is the only version that receives
security fixes.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for a security vulnerability.

Use GitHub's private vulnerability reporting instead: go to the
[Security tab](../../security) of this repository → **Report a
vulnerability**. This opens a private conversation with the maintainer and
avoids exposing details before a fix is available.

Please include:
- A description of the vulnerability and its potential impact
- Steps to reproduce (a minimal repro is ideal)
- The version/commit you tested against

You should get an initial response within a few days. Once a fix is ready,
it will be released and credited to you in the release notes (unless you'd
prefer to stay anonymous — just say so in your report).

## Scope notes

This project handles database credentials (PostgreSQL and Snowflake) and,
in its default configuration, fetches them from Azure Key Vault via a
Managed Identity. Vulnerabilities of particular interest include:

- Credentials or tokens ending up in logs (`log.info`/`log.debug` calls
  should never include passwords, tokens, or PFX bytes — search for
  `# never logged` comments marking places this is already handled
  deliberately)
- SQL injection in the dynamically-built `MERGE`/`INSERT` statements in
  `src/snowflake_sink.py` or `scripts/backfill.py` (table/column names are
  taken from a fixed internal map, not user input — if you find a path
  where that's not true, that's a real bug)
- Anything that could cause the replication slot's LSN to advance before a
  Snowflake write is durably confirmed, resulting in silent data loss
