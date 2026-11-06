"""Unit tests for meshflow.broker.serialization."""

from __future__ import annotations

import struct
import zlib

import pytest

from meshflow.broker.serialization import (
    FRAME_HEADER_SIZE,
    MAGIC,
    MIN_BODY_SIZE,
    Record,
    RecordCorruptionError,
    RecordTruncatedError,
    deserialize_record,
    read_record_at,
    record_size,
    serialize_record,
)
from meshflow.common.types import Event

# ── Helpers ──────────────────────────────────────────────────────────


def _make_record(
    offset: int = 0,
    event_id: str = "evt-1",
    partition_key: str = "key-1",
    payload: bytes = b"hello",
    headers: dict[str, str] | None = None,
    schema_version: int = 1,
    timestamp_ms: int = 1700000000000,
) -> Record:
    return Record(
        offset=offset,
        timestamp_ms=timestamp_ms,
        event_id=event_id,
        schema_version=schema_version,
        partition_key=partition_key,
        headers=headers or {},
        payload=payload,
    )


# ── Round-trip tests ─────────────────────────────────────────────────


class TestSerializationRoundTrip:
    """Tests that serialize → deserialize produces identical Record."""

    def test_basic_round_trip(self) -> None:
        original = _make_record()
        data = serialize_record(original)
        restored = deserialize_record(data)

        assert restored.offset == original.offset
        assert restored.timestamp_ms == original.timestamp_ms
        assert restored.event_id == original.event_id
        assert restored.schema_version == original.schema_version
        assert restored.partition_key == original.partition_key
        assert restored.headers == original.headers
        assert restored.payload == original.payload

    def test_empty_payload_and_headers(self) -> None:
        original = _make_record(payload=b"", headers={})
        data = serialize_record(original)
        restored = deserialize_record(data)

        assert restored.payload == b""
        assert restored.headers == {}

    def test_large_payload(self) -> None:
        """Round-trip with a 10 KB payload."""
        big_payload = b"X" * 10240
        original = _make_record(payload=big_payload)
        data = serialize_record(original)
        restored = deserialize_record(data)

        assert restored.payload == big_payload

    def test_unicode_event_id_and_partition_key(self) -> None:
        original = _make_record(
            event_id="事件-αβγ-🎉",
            partition_key="clé-données-日本語",
        )
        data = serialize_record(original)
        restored = deserialize_record(data)

        assert restored.event_id == "事件-αβγ-🎉"
        assert restored.partition_key == "clé-données-日本語"

    def test_round_trip_with_headers(self) -> None:
        headers = {"content-type": "application/json", "trace-id": "abc-123"}
        original = _make_record(headers=headers)
        data = serialize_record(original)
        restored = deserialize_record(data)

        assert restored.headers == headers


# ── CRC validation ───────────────────────────────────────────────────


class TestCRCValidation:
    """Tests that corrupted data is detected."""

    def test_corrupt_body_byte_raises(self) -> None:
        """Flip a byte in the body → RecordCorruptionError."""
        data = bytearray(serialize_record(_make_record()))
        # Corrupt a byte in the payload region (well past the header)
        corrupt_pos = FRAME_HEADER_SIZE + 10
        data[corrupt_pos] ^= 0xFF

        with pytest.raises(RecordCorruptionError, match="CRC mismatch"):
            deserialize_record(bytes(data))


# ── Invalid magic bytes ──────────────────────────────────────────────


class TestInvalidMagic:
    """Tests for invalid magic bytes."""

    def test_wrong_magic_raises(self) -> None:
        data = bytearray(serialize_record(_make_record()))
        data[0:2] = b"\x00\x00"

        with pytest.raises(RecordCorruptionError, match="Invalid magic"):
            deserialize_record(bytes(data))


# ── Truncated data ───────────────────────────────────────────────────


class TestTruncatedData:
    """Tests for truncated data."""

    def test_truncated_header_raises(self) -> None:
        data = serialize_record(_make_record())
        with pytest.raises(RecordTruncatedError):
            deserialize_record(data[:4])

    def test_truncated_body_raises(self) -> None:
        data = serialize_record(_make_record())
        # Chop off the last 5 bytes (within the body)
        with pytest.raises(RecordTruncatedError):
            deserialize_record(data[:-5])


# ── Unsupported version ─────────────────────────────────────────────


class TestUnsupportedVersion:
    """Tests for unsupported format version."""

    def test_version_2_raises(self) -> None:
        data = bytearray(serialize_record(_make_record()))
        data[2] = 0x02  # version byte

        with pytest.raises(RecordCorruptionError, match="Unsupported format version"):
            deserialize_record(bytes(data))


# ── Record.from_event / to_event round-trip ──────────────────────────


class TestRecordEventRoundTrip:
    """Tests for Record ↔ Event conversion."""

    def test_from_event_to_event_preserves_fields(self) -> None:
        event = Event(
            event_id="e-1",
            topic="orders",
            partition_key="user-42",
            timestamp_ms=1700000000000,
            schema_version=2,
            payload=b"order-data",
            headers={"source": "api"},
        )
        record = Record.from_event(event, offset=7)
        restored_event = record.to_event("orders")

        assert restored_event.event_id == event.event_id
        assert restored_event.topic == event.topic
        assert restored_event.partition_key == event.partition_key
        assert restored_event.timestamp_ms == event.timestamp_ms
        assert restored_event.schema_version == event.schema_version
        assert restored_event.payload == event.payload
        assert restored_event.headers == event.headers

    def test_from_event_sets_offset(self) -> None:
        event = Event.create("t", "k", b"p")
        record = Record.from_event(event, offset=42)
        assert record.offset == 42


# ── record_size matches actual serialized length ─────────────────────


class TestRecordSize:
    """Tests that record_size() matches len(serialize_record())."""

    def test_size_matches_basic_record(self) -> None:
        rec = _make_record()
        assert record_size(rec) == len(serialize_record(rec))

    def test_size_matches_empty_payload(self) -> None:
        rec = _make_record(payload=b"", headers={})
        assert record_size(rec) == len(serialize_record(rec))

    def test_size_matches_large_payload(self) -> None:
        rec = _make_record(payload=b"Y" * 8192)
        assert record_size(rec) == len(serialize_record(rec))

    def test_size_matches_unicode_fields(self) -> None:
        rec = _make_record(event_id="αβγ", partition_key="日本語")
        assert record_size(rec) == len(serialize_record(rec))

    def test_size_matches_with_headers(self) -> None:
        rec = _make_record(headers={"a": "1", "b": "2"})
        assert record_size(rec) == len(serialize_record(rec))

    @pytest.mark.parametrize(
        ("field", "value", "message"),
        [
            ("offset", -1, "offset must fit"),
            ("schema_version", 65536, "schema_version must fit"),
            ("timestamp_ms", 2**63, "timestamp_ms must fit"),
        ],
    )
    def test_numeric_fields_outside_wire_range_are_rejected(
        self, field: str, value: int, message: str
    ) -> None:
        record = _make_record()
        setattr(record, field, value)

        with pytest.raises(ValueError, match=message):
            serialize_record(record)


# ── read_record_at from a file ───────────────────────────────────────


class TestReadRecordAt:
    """Tests for reading a record from a temporary file."""

    def test_read_record_at_from_file(self, tmp_path: object) -> None:
        from pathlib import Path

        tmp = Path(str(tmp_path))
        rec = _make_record(offset=0)
        data = serialize_record(rec)
        file_path = tmp / "test.log"
        file_path.write_bytes(data)

        with open(file_path, "rb") as f:
            restored, consumed = read_record_at(f, 0)

        assert consumed == len(data)
        assert restored.offset == rec.offset
        assert restored.event_id == rec.event_id
        assert restored.payload == rec.payload

    def test_read_multiple_records_from_file(self, tmp_path: object) -> None:
        from pathlib import Path

        tmp = Path(str(tmp_path))
        rec1 = _make_record(offset=0, event_id="e1")
        rec2 = _make_record(offset=1, event_id="e2")
        data1 = serialize_record(rec1)
        data2 = serialize_record(rec2)

        file_path = tmp / "test.log"
        file_path.write_bytes(data1 + data2)

        with open(file_path, "rb") as f:
            r1, consumed1 = read_record_at(f, 0)
            r2, consumed2 = read_record_at(f, consumed1)

        assert r1.event_id == "e1"
        assert r2.event_id == "e2"
        assert consumed1 + consumed2 == len(data1) + len(data2)


# ── Record length boundary checks ───────────────────────────────────


class TestRecordLengthBoundaries:
    """Tests for record length below minimum or exceeding maximum."""

    def test_record_length_below_minimum_raises(self) -> None:
        """Forge a frame with record_length < MIN_BODY_SIZE."""
        bad_length = MIN_BODY_SIZE - 1
        data = MAGIC + struct.pack(">B", 1) + struct.pack(">I", bad_length)
        # Pad enough bytes so it doesn't trigger a truncation error first
        data += b"\x00" * bad_length

        with pytest.raises(RecordCorruptionError, match="below minimum"):
            deserialize_record(data)

    def test_record_length_exceeding_max_raises(self) -> None:
        """Forge a frame with record_length > max_record_bytes."""
        huge_length = 1000
        data = MAGIC + struct.pack(">B", 1) + struct.pack(">I", huge_length)
        data += b"\x00" * huge_length

        with pytest.raises(RecordCorruptionError, match="exceeds maximum"):
            deserialize_record(data, max_record_bytes=500)

    def test_invalid_field_length_raises_corruption_error(self) -> None:
        data = bytearray(serialize_record(_make_record()))
        event_id_length_position = FRAME_HEADER_SIZE + 16
        data[event_id_length_position : event_id_length_position + 2] = b"\xff\xff"

        record_length = int.from_bytes(data[3:7], "big")
        crc_position = FRAME_HEADER_SIZE + record_length - 4
        data[crc_position:] = zlib.crc32(data[FRAME_HEADER_SIZE:crc_position]).to_bytes(4, "big")

        with pytest.raises(RecordCorruptionError, match="event_id extends"):
            deserialize_record(bytes(data))
