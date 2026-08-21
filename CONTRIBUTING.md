# Contributing

Thanks for considering a contribution — issues, PRs, and questions are all welcome.

## Getting set up

```bash
git clone https://github.com/sreenand-kandath/pg-snowflake-cdc.git
cd pg-snowflake-cdc
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Run the tests before and after your change:

```bash
pytest tests/ -v
```

`tests/test_pgoutput_parser.py` builds raw pgoutput binary messages by hand,
so it runs without a live PostgreSQL instance. If you're changing
`pg_replication.py` or `snowflake_sink.py`, `scripts/test_connectivity.py`
and `scripts/check_replication_setup.py` are useful against a real
Postgres/Snowflake instance (a local `docker run postgres:16` with
`wal_level=logical` is enough to test against).

## Making a change

1. Fork the repo and create a branch off `main`.
2. Keep the change focused — smaller PRs get reviewed faster.
3. Add or update tests for anything in `src/` that has testable logic
   (parsing, SQL building, column-name conversion, etc.).
4. Update `README.md` if you change configuration, behavior, or the schema
   example.
5. Add an entry under `[Unreleased]` in `CHANGELOG.md`.
6. Open a PR against `main`. CI (`.github/workflows/ci.yml`) runs the test
   suite and a Docker build automatically.

## Code style

- Match the style already in the file you're editing — this project doesn't
  enforce a formatter, but it is consistent about docstrings-at-the-top,
  `# ── Section ── #` dividers, and structured `log.info(..., extra={...})`
  calls instead of f-string logging.
- Prefer clarity over cleverness — this project is meant to be read
  end-to-end by someone evaluating whether to use it.

## Reporting bugs

Open an issue with:
- What you expected vs. what happened
- PostgreSQL version and Snowflake region (if relevant)
- Relevant log lines (the service logs structured JSON — redact anything
  sensitive before pasting)

## Reporting security issues

Please don't open a public issue for a security vulnerability — see
[SECURITY.md](SECURITY.md) instead.
