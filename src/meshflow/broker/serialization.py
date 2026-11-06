"""Binary record serialization for the MeshFlow storage engine.

Record binary format (all multi-byte integers are big-endian):

    MAGIC:             2 bytes   (0x4D46 = "MF")
    VERSION:           1 byte    (0x01)
    RECORD_LENGTH:     4 bytes   (uint32, total body bytes including CRC32)
    ── body ──────────────────────────────────────────────────
    OFFSET:            8 bytes   (uint64)
    TIMESTAMP_MS:      8 bytes   (int64)
    EVENT_ID_LEN:      2 bytes   (uint16)
    EVENT_ID:          variable  (UTF-8 bytes)
    SCHEMA_VERSION:    2 bytes   (uint16)
    PARTITION_KEY_LEN: 2 bytes   (uint16)
    PARTITION_KEY:     variable  (UTF-8 bytes)
    HEADER_LEN:        4 bytes   (uint32)
    HEADERS:           variable  (JSON UTF-8 bytes)
    PAYLOAD_LEN:       4 bytes   (uint32)
    PAYLOAD:           variable  (opaque bytes)
    CRC32:             4 bytes   (uint32, CRC of body excluding CRC field itself)

The RECORD_LENGTH field counts all bytes from OFFSET through CRC32 inclusive.
The CRC32 is computed over all body bytes from OFFSET through PAYLOAD inclusive
(i.e., everything in the body except the CRC32 field itself).

See docs/storage-format.md for the full specification.
"""

from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass
from typing import BinaryIO

from meshflow.common.types import Event

# ── Constants ──────────────────────────────────────────────────────────

MAGIC = b"\x4d\x46"  # "MF"
FORMAT_VERSION = 1
FRAME_HEADER_SIZE = 7  # MAGIC(2) + VERSION(1) + RECORD_LENGTH(4)
CRC_SIZE = 4

# Minimum body size: fixed fields only, all variable-length fields empty
#   OFFSET(8) + TIMESTAMP(8) + EVENT_ID_LEN(2) + SCHEMA_VERSION(2)
#   + PARTITION_KEY_LEN(2) + HEADER_LEN(4) + PAYLOAD_LEN(4) + CRC32(4)
MIN_BODY_SIZE = 8 + 8 + 2 + 2 + 2 + 4 + 4 + 4  # 34 bytes

# Safety limit: reject records claiming to be larger than this
DEFAULT_MAX_RECORD_BYTES = 16 * 1024 * 1024  # 16 MB


class RecordCorruptionError(Exception):
    """Raised when a record fails integrity validation."""


class RecordTruncatedError(Exception):
    """Raised when a record is incomplete (truncated tail)."""


# ── Record dataclass ──────────────────────────────────────────────────


@dataclass(slots=True)
class Record:
    """A single record as stored in a partition log segment.

    This is the broker's internal representation, separate from the
    application-facing Event type.
    """

    offset: int
    timestamp_ms: int
    event_id: str
    schema_version: int
    partition_key: str
    headers: dict[str, str]
    payload: bytes

    @classmethod
    def from_event(cls, event: Event, offset: int) -> Record:
        """Create a Record from an Event and an assigned offset."""
        return cls(
            offset=offset,
            timestamp_ms=event.timestamp_ms,
            event_id=event.event_id,
            schema_version=event.schema_version,
            partition_key=event.partition_key,
            headers=dict(event.headers),
            payload=event.payload,
        )

    def to_event(self, topic: str) -> Event:
        """Reconstruct an Event from this record. Topic is not stored in the record."""
        return Event(
            event_id=self.event_id,
            topic=topic,
            partition_key=self.partition_key,
            timestamp_ms=self.timestamp_ms,
            schema_version=self.schema_version,
            payload=self.payload,
            headers=dict(self.headers),
        )


# ── Serialization ─────────────────────────────────────────────────────


def serialize_record(record: Record) -> bytes:
    """Serialize a Record to its binary on-disk format.

    Returns:
        The complete binary representation including frame header and CRC32.

    Raises:
        ValueError: If any variable-length field exceeds its length prefix capacity.
    """
    if not 0 <= record.offset <= 0xFFFFFFFFFFFFFFFF:
        raise ValueError("offset must fit in an unsigned 64-bit integer")
    if not -(2**63) <= record.timestamp_ms < 2**63:
        raise ValueError("timestamp_ms must fit in a signed 64-bit integer")
    if not 0 <= record.schema_version <= 0xFFFF:
        raise ValueError("schema_version must fit in an unsigned 16-bit integer")
    if not isinstance(record.headers, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in record.headers.items()
    ):
        raise ValueError("record headers must contain only string keys and values")
    if not isinstance(record.payload, bytes):
        raise ValueError("record payload must be bytes")

    event_id_bytes = record.event_id.encode("utf-8")
    partition_key_bytes = record.partition_key.encode("utf-8")
    headers_bytes = json.dumps(record.headers, separators=(",", ":")).encode("utf-8")

    if len(event_id_bytes) > 0xFFFF:
        raise ValueError(f"event_id too long: {len(event_id_bytes)} bytes (max 65535)")
    if len(partition_key_bytes) > 0xFFFF:
        raise ValueError(f"partition_key too long: {len(partition_key_bytes)} bytes (max 65535)")
    if len(headers_bytes) > 0xFFFFFFFF:
        raise ValueError(f"headers too long: {len(headers_bytes)} bytes")
    if len(record.payload) > 0xFFFFFFFF:
        raise ValueError(f"payload too long: {len(record.payload)} bytes")

    # Build the body (everything between RECORD_LENGTH and CRC32, inclusive of CRC)
    body_without_crc = (
        struct.pack(">Q", record.offset)
        + struct.pack(">q", record.timestamp_ms)
        + struct.pack(">H", len(event_id_bytes))
        + event_id_bytes
        + struct.pack(">H", record.schema_version)
        + struct.pack(">H", len(partition_key_bytes))
        + partition_key_bytes
        + struct.pack(">I", len(headers_bytes))
        + headers_bytes
        + struct.pack(">I", len(record.payload))
        + record.payload
    )

    crc = zlib.crc32(body_without_crc) & 0xFFFFFFFF
    body = body_without_crc + struct.pack(">I", crc)

    # Frame header: MAGIC + VERSION + RECORD_LENGTH
    frame_header = MAGIC + struct.pack(">B", FORMAT_VERSION) + struct.pack(">I", len(body))

    return frame_header + body


def deserialize_record(
    data: bytes,
    max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES,
) -> Record:
    """Deserialize a Record from its complete binary representation.

    Args:
        data: Complete binary record data starting from MAGIC.
        max_record_bytes: Maximum allowed record size for safety.

    Returns:
        The deserialized Record.

    Raises:
        RecordCorruptionError: If magic, version, or CRC validation fails.
        RecordTruncatedError: If the data is too short.
    """
    if len(data) < FRAME_HEADER_SIZE:
        raise RecordTruncatedError(
            f"Data too short for frame header: {len(data)} < {FRAME_HEADER_SIZE}"
        )

    magic = data[0:2]
    if magic != MAGIC:
        raise RecordCorruptionError(f"Invalid magic bytes: {magic!r} (expected {MAGIC!r})")

    version = data[2]
    if version != FORMAT_VERSION:
        raise RecordCorruptionError(
            f"Unsupported format version: {version} (expected {FORMAT_VERSION})"
        )

    (record_length,) = struct.unpack(">I", data[3:7])

    if record_length > max_record_bytes:
        raise RecordCorruptionError(
            f"Record length {record_length} exceeds maximum {max_record_bytes}"
        )
    if record_length < MIN_BODY_SIZE:
        raise RecordCorruptionError(f"Record length {record_length} below minimum {MIN_BODY_SIZE}")

    total_size = FRAME_HEADER_SIZE + record_length
    if len(data) < total_size:
        raise RecordTruncatedError(f"Data too short: {len(data)} < {total_size} (header + body)")

    body = data[FRAME_HEADER_SIZE:total_size]

    # Validate CRC32: CRC covers everything except the last 4 bytes (the CRC itself)
    body_without_crc = body[:-CRC_SIZE]
    (stored_crc,) = struct.unpack(">I", body[-CRC_SIZE:])
    computed_crc = zlib.crc32(body_without_crc) & 0xFFFFFFFF
    if stored_crc != computed_crc:
        raise RecordCorruptionError(
            f"CRC mismatch: stored=0x{stored_crc:08X}, computed=0x{computed_crc:08X}"
        )

    # Parse body fields, checking each declared length before slicing.
    pos = 0

    def take(size: int, field: str) -> bytes:
        nonlocal pos
        end = pos + size
        if end > len(body_without_crc):
            raise RecordCorruptionError(f"{field} extends beyond the record body")
        value = body_without_crc[pos:end]
        pos = end
        return value

    (offset,) = struct.unpack(">Q", take(8, "offset"))
    (timestamp_ms,) = struct.unpack(">q", take(8, "timestamp"))

    (event_id_len,) = struct.unpack(">H", take(2, "event_id length"))
    try:
        event_id = take(event_id_len, "event_id").decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RecordCorruptionError("event_id is not valid UTF-8") from exc

    (schema_version,) = struct.unpack(">H", take(2, "schema version"))

    (partition_key_len,) = struct.unpack(">H", take(2, "partition_key length"))
    try:
        partition_key = take(partition_key_len, "partition_key").decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RecordCorruptionError("partition_key is not valid UTF-8") from exc

    (header_len,) = struct.unpack(">I", take(4, "headers length"))
    headers_bytes = take(header_len, "headers")
    try:
        headers = json.loads(headers_bytes.decode("utf-8")) if header_len > 0 else {}
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecordCorruptionError("headers are not valid UTF-8 JSON") from exc
    if not isinstance(headers, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in headers.items()
    ):
        raise RecordCorruptionError("headers must be a JSON object of string values")

    (payload_len,) = struct.unpack(">I", take(4, "payload length"))
    payload = take(payload_len, "payload")
    if pos != len(body_without_crc):
        raise RecordCorruptionError("Unexpected trailing bytes in record body")

    return Record(
        offset=offset,
        timestamp_ms=timestamp_ms,
        event_id=event_id,
        schema_version=schema_version,
        partition_key=partition_key,
        headers=headers,
        payload=payload,
    )


# ── Stream reading ────────────────────────────────────────────────────


def read_record_at(
    f: BinaryIO,
    position: int,
    max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES,
) -> tuple[Record, int]:
    """Read a single record from a file at the given byte position.

    Args:
        f: An open binary file.
        position: Byte offset to start reading from.
        max_record_bytes: Maximum allowed record size.

    Returns:
        A tuple of (Record, total_bytes_consumed).

    Raises:
        RecordCorruptionError: If the record is corrupt.
        RecordTruncatedError: If the record is incomplete.
    """
    f.seek(position)

    header_data = f.read(FRAME_HEADER_SIZE)
    if len(header_data) == 0:
        raise RecordTruncatedError("No data at position")
    if len(header_data) < FRAME_HEADER_SIZE:
        raise RecordTruncatedError(
            f"Incomplete frame header at position {position}: "
            f"{len(header_data)} < {FRAME_HEADER_SIZE}"
        )

    magic = header_data[0:2]
    if magic != MAGIC:
        raise RecordCorruptionError(f"Invalid magic at position {position}: {magic!r}")

    version = header_data[2]
    if version != FORMAT_VERSION:
        raise RecordCorruptionError(f"Unsupported version at position {position}: {version}")

    (record_length,) = struct.unpack(">I", header_data[3:7])

    if record_length > max_record_bytes:
        raise RecordCorruptionError(
            f"Record at position {position} claims length {record_length} "
            f"exceeding maximum {max_record_bytes}"
        )
    if record_length < MIN_BODY_SIZE:
        raise RecordCorruptionError(
            f"Record at position {position} claims length {record_length} "
            f"below minimum {MIN_BODY_SIZE}"
        )

    body_data = f.read(record_length)
    if len(body_data) < record_length:
        raise RecordTruncatedError(
            f"Incomplete record body at position {position}: {len(body_data)} < {record_length}"
        )

    full_data = header_data + body_data
    record = deserialize_record(full_data, max_record_bytes)
    return record, len(full_data)


def record_size(record: Record) -> int:
    """Calculate the total serialized size of a record in bytes without serializing it.

    This is useful for size checks before committing to serialization.
    """
    event_id_len = len(record.event_id.encode("utf-8"))
    partition_key_len = len(record.partition_key.encode("utf-8"))
    headers_len = len(json.dumps(record.headers, separators=(",", ":")).encode("utf-8"))
    payload_len = len(record.payload)

    body_size = (
        8  # offset
        + 8  # timestamp
        + 2
        + event_id_len  # event_id_len + event_id
        + 2  # schema_version
        + 2
        + partition_key_len  # partition_key_len + partition_key
        + 4
        + headers_len  # header_len + headers
        + 4
        + payload_len  # payload_len + payload
        + CRC_SIZE  # crc32
    )

    return FRAME_HEADER_SIZE + body_size
