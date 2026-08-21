"""
Unit tests for src/pgoutput_parser.py.

These build raw pgoutput binary messages by hand (per the wire protocol
documented at the top of pgoutput_parser.py) rather than requiring a live
PostgreSQL instance, so they run anywhere with just `pytest`.
"""

import struct
import datetime

import pytest

from src.pgoutput_parser import PgOutputParser, _PG_EPOCH


def _begin_msg(commit_lsn: int = 100, ts: datetime.datetime | None = None) -> bytes:
    ts = ts or datetime.datetime.now(datetime.timezone.utc)
    ts_us = int((ts - _PG_EPOCH).total_seconds() * 1_000_000)
    body = struct.pack(">Q", commit_lsn) + struct.pack(">q", ts_us) + struct.pack(">I", 42)
    return b"B" + body


def _relation_msg(oid: int, namespace: str, table: str, columns: list[tuple[str, int, bool]]) -> bytes:
    body = struct.pack(">I", oid)
    body += namespace.encode() + b"\x00"
    body += table.encode() + b"\x00"
    body += bytes([1])  # replica identity
    body += struct.pack(">H", len(columns))
    for name, type_oid, is_key in columns:
        body += bytes([1 if is_key else 0])
        body += name.encode() + b"\x00"
        body += struct.pack(">I", type_oid)
        body += struct.pack(">i", -1)  # type modifier
    return b"R" + body


def _tuple_bytes(values: list[str | None]) -> bytes:
    out = struct.pack(">H", len(values))
    for v in values:
        if v is None:
            out += b"n"
        else:
            encoded = v.encode()
            out += b"t" + struct.pack(">I", len(encoded)) + encoded
    return out


def _insert_msg(oid: int, after: list[str | None]) -> bytes:
    body = struct.pack(">I", oid) + b"N" + _tuple_bytes(after)
    return b"I" + body


def _update_msg(oid: int, after: list[str | None]) -> bytes:
    body = struct.pack(">I", oid) + b"N" + _tuple_bytes(after)
    return b"U" + body


def _delete_msg(oid: int, before: list[str | None]) -> bytes:
    body = struct.pack(">I", oid) + b"K" + _tuple_bytes(before)
    return b"D" + body


@pytest.fixture
def parser() -> PgOutputParser:
    return PgOutputParser()


def _prime(parser: PgOutputParser, oid: int = 1) -> None:
    parser.parse(_begin_msg(commit_lsn=500), lsn=100)
    parser.parse(
        _relation_msg(oid, "public", "customers", [
            ("id", 23, True), ("tenant_id", 25, False), ("email", 25, False),
        ]),
        lsn=101,
    )


def test_insert_produces_change_event(parser: PgOutputParser) -> None:
    _prime(parser)
    event = parser.parse(_insert_msg(1, ["1", "acme", "a@example.com"]), lsn=102)

    assert event is not None
    assert event.op == "c"
    assert event.table == "customers"
    assert event.schema == "public"
    assert event.after == {"id": "1", "tenant_id": "acme", "email": "a@example.com"}
    assert event.before is None
    assert event.commit_lsn == 500


def test_update_produces_after_state(parser: PgOutputParser) -> None:
    _prime(parser)
    event = parser.parse(_update_msg(1, ["1", "acme", "new@example.com"]), lsn=103)

    assert event is not None
    assert event.op == "u"
    assert event.after["email"] == "new@example.com"


def test_delete_produces_before_state(parser: PgOutputParser) -> None:
    _prime(parser)
    event = parser.parse(_delete_msg(1, ["1", "acme", "a@example.com"]), lsn=104)

    assert event is not None
    assert event.op == "d"
    assert event.after is None
    assert event.before["id"] == "1"


def test_null_column_is_none(parser: PgOutputParser) -> None:
    _prime(parser)
    event = parser.parse(_insert_msg(1, ["2", "acme", None]), lsn=105)

    assert event.after["email"] is None


def test_unknown_relation_oid_returns_none(parser: PgOutputParser) -> None:
    # No relation message seen for oid=99 — should skip, not crash.
    event = parser.parse(_insert_msg(99, ["1"]), lsn=106)
    assert event is None


def test_empty_payload_returns_none(parser: PgOutputParser) -> None:
    assert parser.parse(b"", lsn=1) is None


def test_relation_is_reusable_across_messages(parser: PgOutputParser) -> None:
    """A Relation message arrives once per session; later Insert/Update/Delete
    messages reference it by OID without repeating the schema."""
    _prime(parser)
    e1 = parser.parse(_insert_msg(1, ["1", "acme", "a@example.com"]), lsn=110)
    e2 = parser.parse(_update_msg(1, ["1", "acme", "b@example.com"]), lsn=111)

    assert e1.table == e2.table == "customers"
    assert e1.after["email"] != e2.after["email"]
