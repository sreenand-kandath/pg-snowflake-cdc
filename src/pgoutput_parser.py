"""
pgoutput_parser.py
Parses PostgreSQL pgoutput logical replication binary messages into plain dicts.

pgoutput is built into PostgreSQL 10+ (no extension required) and works on
every managed Postgres offering that supports logical replication, including
ones (like Azure Database for PostgreSQL Flexible Server) that don't let you
install third-party decoding plugins such as wal2json.

Protocol reference:
  https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
"""

import datetime
import struct
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# PostgreSQL epoch: 2000-01-01 00:00:00 UTC
_PG_EPOCH = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)


@dataclass
class _ColumnInfo:
    name: str
    type_oid: int
    is_key: bool = False


@dataclass
class _RelationInfo:
    oid: int
    namespace: str
    table: str
    columns: List[_ColumnInfo] = field(default_factory=list)


@dataclass
class ChangeEvent:
    op: str           # 'c' insert, 'u' update, 'd' delete
    schema: str
    table: str
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    lsn: int
    ts_ms: int        # Unix milliseconds
    commit_lsn: int = 0  # LSN of the transaction's COMMIT record (from BEGIN final_lsn)


def _read_cstring(data: bytes, offset: int) -> Tuple[str, int]:
    """Read a null-terminated UTF-8 string, return (value, new_offset)."""
    try:
        end = data.index(b"\x00", offset)
    except ValueError:
        raise ValueError(
            f"pgoutput: null terminator not found in string at offset {offset} "
            f"(message length {len(data)})"
        )
    return data[offset:end].decode("utf-8"), end + 1


def _read_tuple(
    data: bytes, offset: int, relation: _RelationInfo
) -> Tuple[Dict[str, Any], int]:
    """Parse a TupleData block and return (row_dict, new_offset)."""
    num_cols = struct.unpack_from(">H", data, offset)[0]
    offset += 2
    row: Dict[str, Any] = {}

    for i in range(num_cols):
        kind = chr(data[offset])
        offset += 1
        col_name = relation.columns[i].name if i < len(relation.columns) else f"col_{i}"

        if kind == "n":
            row[col_name] = None
        elif kind == "u":
            row[col_name] = None  # unchanged TOAST — value not available
        elif kind in ("t", "b"):
            col_len = struct.unpack_from(">I", data, offset)[0]
            offset += 4
            raw = data[offset : offset + col_len]
            offset += col_len
            row[col_name] = raw.decode("utf-8", errors="replace") if kind == "t" else raw

    return row, offset


class PgOutputParser:
    """Stateful parser — must persist across messages within a session
    because Relation messages arrive once and are referenced by OID later."""

    def __init__(self) -> None:
        self._relations: Dict[int, _RelationInfo] = {}
        self._current_ts_ms: int = 0
        self._current_commit_lsn: int = 0

    def parse(self, data: bytes, lsn: int) -> Optional[ChangeEvent]:
        """Parse one raw pgoutput message. Returns a ChangeEvent or None."""
        if not data:
            return None
        if len(data) < 1:
            return None

        msg_type = chr(data[0])
        body = data[1:]

        try:
            if msg_type == "B":
                self._handle_begin(body)
            elif msg_type == "R":
                self._handle_relation(body)
            elif msg_type == "I":
                return self._handle_insert(body, lsn)
            elif msg_type == "U":
                return self._handle_update(body, lsn)
            elif msg_type == "D":
                return self._handle_delete(body, lsn)
            # C, O, Y, T, M — skip silently
        except (struct.error, ValueError, UnicodeDecodeError, IndexError) as exc:
            import logging
            logging.getLogger(__name__).warning(
                "pgoutput parse error — skipping message",
                extra={"msg_type": msg_type, "lsn": lsn,
                       "msg_len": len(data), "error": str(exc)},
            )
        return None

    # ------------------------------------------------------------------ #
    #  Private handlers                                                  #
    # ------------------------------------------------------------------ #

    def _handle_begin(self, data: bytes) -> None:
        # Int64 final_lsn (commit LSN), Int64 commit_ts (µs since PG epoch), Int32 xid
        final_lsn_raw = struct.unpack_from(">Q", data, 0)[0]
        self._current_commit_lsn = final_lsn_raw
        ts_us = struct.unpack_from(">q", data, 8)[0]
        ts_dt = _PG_EPOCH + datetime.timedelta(microseconds=ts_us)
        self._current_ts_ms = int(ts_dt.timestamp() * 1000)

    def _handle_relation(self, data: bytes) -> None:
        oid = struct.unpack_from(">I", data, 0)[0]
        offset = 4
        namespace, offset = _read_cstring(data, offset)
        table, offset = _read_cstring(data, offset)
        # replica identity (1 byte), skip
        offset += 1
        num_cols = struct.unpack_from(">H", data, offset)[0]
        offset += 2

        columns: List[_ColumnInfo] = []
        for _ in range(num_cols):
            flags = data[offset]
            offset += 1
            col_name, offset = _read_cstring(data, offset)
            type_oid = struct.unpack_from(">I", data, offset)[0]
            offset += 4 + 4  # type_oid + type_modifier
            columns.append(_ColumnInfo(name=col_name, type_oid=type_oid, is_key=bool(flags & 1)))

        self._relations[oid] = _RelationInfo(
            oid=oid, namespace=namespace, table=table, columns=columns
        )

    def _handle_insert(self, data: bytes, lsn: int) -> Optional[ChangeEvent]:
        oid = struct.unpack_from(">I", data, 0)[0]
        relation = self._relations.get(oid)
        if not relation or chr(data[4]) != "N":
            return None
        after, _ = _read_tuple(data, 5, relation)
        return self._event("c", relation, None, after, lsn)

    def _handle_update(self, data: bytes, lsn: int) -> Optional[ChangeEvent]:
        oid = struct.unpack_from(">I", data, 0)[0]
        relation = self._relations.get(oid)
        if not relation:
            return None

        offset = 4
        before: Optional[Dict[str, Any]] = None
        kind = chr(data[offset])
        offset += 1

        if kind in ("K", "O"):  # key tuple or full old tuple
            before, offset = _read_tuple(data, offset, relation)
            kind = chr(data[offset])
            offset += 1

        after: Optional[Dict[str, Any]] = None
        if kind == "N":
            after, _ = _read_tuple(data, offset, relation)

        return self._event("u", relation, before, after, lsn)

    def _handle_delete(self, data: bytes, lsn: int) -> Optional[ChangeEvent]:
        oid = struct.unpack_from(">I", data, 0)[0]
        relation = self._relations.get(oid)
        if not relation:
            return None

        offset = 4
        kind = chr(data[offset])
        offset += 1
        before: Optional[Dict[str, Any]] = None
        if kind in ("K", "O"):
            before, _ = _read_tuple(data, offset, relation)

        return self._event("d", relation, before, None, lsn)

    def _event(
        self,
        op: str,
        relation: _RelationInfo,
        before: Optional[Dict[str, Any]],
        after: Optional[Dict[str, Any]],
        lsn: int,
    ) -> ChangeEvent:
        return ChangeEvent(
            op=op,
            schema=relation.namespace,
            table=relation.table,
            before=before,
            after=after,
            lsn=lsn,
            ts_ms=self._current_ts_ms,
            commit_lsn=self._current_commit_lsn,
        )
