# MeshFlow Storage Format Specification

> **Status:** Authoritative specification
> **Applies to:** Format Version 1 (`0x01`)

This document defines the binary on-disk format used by the MeshFlow broker
storage engine. All implementations that read or write MeshFlow segment files
and index files **must** conform to this specification.

## Conventions

| Convention | Meaning |
|---|---|
| **Byte order** | Big-endian (`>` in Python `struct` format strings) for all multi-byte integers |
| **String encoding** | UTF-8 for all text fields (`event_id`, `partition_key`, `headers`) |
| **CRC algorithm** | `zlib.crc32` (ISO 3309 / ITU-T V.42 polynomial), masked to 32 bits (`& 0xFFFFFFFF`) |
| **Offset numbering** | Zero-based, monotonically increasing uint64 |

---

## 1. Record Binary Format

Every record stored in a segment file consists of a **frame header** followed
by a **body**. The frame header provides framing (magic bytes, version, length)
and the body contains the record fields and a trailing CRC32 checksum.

### 1.1 Complete Record Layout

```
 Byte
 Offset   Field               Size       Type      Description
 ─────────────────────────────────────────────────────────────────────
  ┌─── FRAME HEADER (7 bytes) ─────────────────────────────────────┐
  │ 0x00   MAGIC               2 bytes    bytes     0x4D46 ("MF")  │
  │ 0x02   VERSION             1 byte     uint8     0x01           │
  │ 0x03   RECORD_LENGTH       4 bytes    uint32    Body size      │
  └────────────────────────────────────────────────────────────────┘
  ┌─── BODY (RECORD_LENGTH bytes) ─────────────────────────────────┐
  │ 0x07   OFFSET              8 bytes    uint64    Record offset  │
  │ 0x0F   TIMESTAMP_MS        8 bytes    int64     Unix epoch ms  │
  │ 0x17   EVENT_ID_LEN        2 bytes    uint16    Length of E.ID │
  │ 0x19   EVENT_ID            variable   UTF-8     Event ID       │
  │  +E    SCHEMA_VERSION      2 bytes    uint16    Schema version │
  │  +E+2  PARTITION_KEY_LEN   2 bytes    uint16    Length of P.K. │
  │  +E+4  PARTITION_KEY       variable   UTF-8     Partition key  │
  │  +P    HEADER_LEN          4 bytes    uint32    Length of hdrs │
  │  +P+4  HEADERS             variable   UTF-8     JSON object    │
  │  +H    PAYLOAD_LEN         4 bytes    uint32    Length of data  │
  │  +H+4  PAYLOAD             variable   bytes     Opaque payload │
  │  ...   CRC32               4 bytes    uint32    Checksum       │
  └────────────────────────────────────────────────────────────────┘

 Where:
   E = EVENT_ID_LEN
   P = offset after PARTITION_KEY (E + 2 + 2 + PARTITION_KEY_LEN)
   H = offset after HEADERS (P + 4 + HEADER_LEN)
```

### 1.2 ASCII Byte Diagram

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|     MAGIC (0x4D46)    | VER   |        RECORD_LENGTH          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         OFFSET (uint64)                       |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                      TIMESTAMP_MS (int64)                     |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  EVENT_ID_LEN |          EVENT_ID (variable UTF-8) ...        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| SCHEMA_VER    | P_KEY_LEN     |  PARTITION_KEY (variable) ... |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|           HEADER_LEN          |    HEADERS (JSON UTF-8) ...   |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|           PAYLOAD_LEN         |    PAYLOAD (opaque) ...       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|            CRC32              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### 1.3 Field Reference

| Field | Struct Format | Size | Description |
|---|---|---|---|
| `MAGIC` | raw bytes | 2 | Always `0x4D46` (ASCII `"MF"`). Identifies a MeshFlow record frame. |
| `VERSION` | `>B` | 1 | Format version. Currently `0x01`. |
| `RECORD_LENGTH` | `>I` | 4 | Total number of bytes in the body, from `OFFSET` through `CRC32` inclusive. |
| `OFFSET` | `>Q` | 8 | Monotonically increasing record offset (uint64). |
| `TIMESTAMP_MS` | `>q` | 8 | Event timestamp as milliseconds since Unix epoch (signed int64). |
| `EVENT_ID_LEN` | `>H` | 2 | Length of `EVENT_ID` in bytes. Max 65 535. |
| `EVENT_ID` | raw bytes | variable | UTF-8 encoded event identifier. |
| `SCHEMA_VERSION` | `>H` | 2 | Application-level schema version (uint16). |
| `PARTITION_KEY_LEN` | `>H` | 2 | Length of `PARTITION_KEY` in bytes. Max 65 535. |
| `PARTITION_KEY` | raw bytes | variable | UTF-8 encoded partition key. |
| `HEADER_LEN` | `>I` | 4 | Length of `HEADERS` in bytes. Max 4 294 967 295. |
| `HEADERS` | raw bytes | variable | Compact JSON object (`{"k":"v",...}`) encoded as UTF-8. Empty headers → `HEADER_LEN` = 0. |
| `PAYLOAD_LEN` | `>I` | 4 | Length of `PAYLOAD` in bytes. Max 4 294 967 295. |
| `PAYLOAD` | raw bytes | variable | Opaque application payload. |
| `CRC32` | `>I` | 4 | CRC-32 checksum of the body, excluding the CRC32 field itself. |

---

## 2. Record Framing

### 2.1 Frame Header

The first 7 bytes of every record form the **frame header**:

```
┌────────┬─────────┬────────────────┐
│ MAGIC  │ VERSION │ RECORD_LENGTH  │
│ 2 B    │ 1 B     │ 4 B            │
│ 0x4D46 │ 0x01    │ uint32         │
└────────┴─────────┴────────────────┘
     7 bytes total
```

- **MAGIC** (`0x4D46`): Validates that the reader is aligned on a record
  boundary. Any other value indicates corruption or misalignment.
- **VERSION** (`0x01`): Allows future format evolution. Readers **must**
  reject unknown versions.
- **RECORD_LENGTH**: The number of bytes that follow the frame header. This
  counts all body bytes from `OFFSET` through `CRC32` inclusive.

### 2.2 Total Frame Size

```
total_frame_size = FRAME_HEADER_SIZE + RECORD_LENGTH
                 = 7 + RECORD_LENGTH
```

### 2.3 Sequential Reading

To read records sequentially from a segment file:

```
position = 0
while position < file_size:
    1. Read 7 bytes at `position` → frame header
    2. Validate MAGIC = 0x4D46
    3. Validate VERSION = 0x01
    4. Parse RECORD_LENGTH (uint32, big-endian)
    5. Validate: MIN_BODY_SIZE ≤ RECORD_LENGTH ≤ max_record_bytes
    6. Read RECORD_LENGTH bytes → body
    7. Validate CRC32 (last 4 bytes of body)
    8. Parse record fields from body
    9. position += 7 + RECORD_LENGTH
```

### 2.4 Minimum Body Size

The minimum valid body consists of only the fixed-size fields with all
variable-length fields having zero length:

```
MIN_BODY_SIZE = OFFSET(8) + TIMESTAMP_MS(8) + EVENT_ID_LEN(2)
              + SCHEMA_VERSION(2) + PARTITION_KEY_LEN(2)
              + HEADER_LEN(4) + PAYLOAD_LEN(4) + CRC32(4)
              = 34 bytes
```

Any `RECORD_LENGTH` less than 34 **must** be rejected as corrupt.

---

## 3. CRC32 Checksum

### 3.1 Algorithm

The checksum uses the CRC-32 algorithm as implemented by Python's `zlib.crc32`
(ISO 3309 / ITU-T V.42 polynomial), masked to an unsigned 32-bit value:

```python
crc = zlib.crc32(body_without_crc) & 0xFFFFFFFF
```

### 3.2 Covered Bytes

```
┌─── Frame Header (NOT covered by CRC) ──┐
│ MAGIC │ VERSION │ RECORD_LENGTH          │
└─────────────────────────────────────────┘
┌─── Body ────────────────────────────────┐
│                                         │
│  ┌─── CRC input (covered) ───────────┐  │
│  │ OFFSET                            │  │
│  │ TIMESTAMP_MS                      │  │
│  │ EVENT_ID_LEN + EVENT_ID           │  │
│  │ SCHEMA_VERSION                    │  │
│  │ PARTITION_KEY_LEN + PARTITION_KEY │  │
│  │ HEADER_LEN + HEADERS             │  │
│  │ PAYLOAD_LEN + PAYLOAD            │  │
│  └────────────────────────────────────┘  │
│  ┌─── NOT covered ──────────────────┐   │
│  │ CRC32 (4 bytes)                  │   │
│  └───────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

- The CRC covers all body bytes **excluding** the 4-byte CRC32 field itself.
- The frame header (MAGIC, VERSION, RECORD_LENGTH) is **not** covered by the CRC.
- The CRC is stored as a big-endian uint32 as the last 4 bytes of the body.

### 3.3 Validation

On read, the CRC is validated by:

```python
body = data[FRAME_HEADER_SIZE : FRAME_HEADER_SIZE + record_length]
body_without_crc = body[:-4]
stored_crc = struct.unpack(">I", body[-4:])[0]
computed_crc = zlib.crc32(body_without_crc) & 0xFFFFFFFF
assert stored_crc == computed_crc  # RecordCorruptionError on mismatch
```

---

## 4. Segment Files

### 4.1 Naming Convention

Segment files and their companion index files follow a strict naming pattern:

```
segment-NNNNNNNNNN.log    ← record data (10-digit zero-padded base offset)
index-NNNNNNNNNN.idx      ← sparse index for the segment
```

**Examples:**

| Base Offset | Segment File | Index File |
|---|---|---|
| 0 | `segment-0000000000.log` | `index-0000000000.idx` |
| 42 | `segment-0000000042.log` | `index-0000000042.idx` |
| 1000 | `segment-0000001000.log` | `index-0000001000.idx` |

The **base offset** is the offset of the first record stored in that segment.

### 4.2 Directory Layout

```
data/<broker_id>/topics/<topic>/partition-<id>/
    segment-0000000000.log
    index-0000000000.idx
    segment-0000000042.log
    index-0000000042.idx
    ...
```

### 4.3 Append-Only Semantics

- Records are **only appended** to the end of the active (most recent) segment.
  Existing data is never modified in-place during normal operation.
- Each record is assigned a monotonically increasing offset by the partition log.
- After writing, the file is flushed and `fsync`-ed according to the configured
  policy (`every_write`, `interval`, or `os`).

Acknowledgement durability is policy-dependent:

| Policy | Append acknowledgement |
|---|---|
| `every_write` | Returned only after the record is flushed and `fsync` completes. |
| `interval` | A write that reaches the configured interval is flushed and `fsync`-ed before it returns; earlier writes may be acknowledged before that sync. The interval is checked on appends, so an idle log does not run a background timer. |
| `os` | Returned after the file write succeeds, without an explicit per-append flush or `fsync`; buffered or OS-cached data can be lost on a crash. |

Closing or explicitly flushing a segment flushes and `fsync`-s it regardless
of policy. None of these policies provides replicated durability.

### 4.4 Rotation Policy

The active segment is **rotated** (closed, and a new segment created) when
either condition is met:

| Condition | Default | Description |
|---|---|---|
| **Size limit** | 64 MB (`max_bytes`) | `segment.byte_size >= max_bytes` |
| **Age limit** | 1 hour (`max_age_ms`) | `current_time - segment.created_ms >= max_age_ms` |

Rotation only occurs before a new append when the active segment already
contains at least one record (`record_count > 0`).

On rotation:
1. The current segment is flushed, `fsync`-ed, and closed.
2. Its sparse index is saved to disk.
3. A new segment is created with `base_offset` equal to the partition's
   `next_offset`.

### 4.5 Segment File Structure

A segment file is simply a concatenation of serialized records with no
file-level header or footer:

```
┌──────────────────────────────────┐
│ Record 0  (frame header + body)  │
├──────────────────────────────────┤
│ Record 1  (frame header + body)  │
├──────────────────────────────────┤
│ Record 2  (frame header + body)  │
├──────────────────────────────────┤
│              ...                 │
├──────────────────────────────────┤
│ Record N  (frame header + body)  │
└──────────────────────────────────┘
```

There is no segment-level magic, header, or trailer. Record-level framing
(MAGIC + VERSION) provides alignment verification.

---

## 5. Sparse Index Format

Each segment has a companion sparse index file (`.idx`) that maps selected
record offsets to their byte positions within the segment file. The index is
an **optimization** — the segment file is the source of truth, and the index
can always be rebuilt by scanning the segment.

### 5.1 Index File Layout

```
 Byte
 Offset   Field               Size       Type      Description
 ──────────────────────────────────────────────────────────────
  ┌─── INDEX HEADER (7 bytes) ─────────────────────────────────┐
  │ 0x00   INDEX_MAGIC          2 bytes    bytes     0x4D49    │
  │ 0x02   INDEX_VERSION        1 byte     uint8     0x01      │
  │ 0x03   ENTRY_COUNT          4 bytes    uint32    N entries │
  └────────────────────────────────────────────────────────────┘
  ┌─── ENTRIES (16 bytes each × ENTRY_COUNT) ──────────────────┐
  │ +0     OFFSET               8 bytes    uint64    Record    │
  │ +8     POSITION             8 bytes    uint64    Byte pos  │
  ├────────────────────────────────────────────────────────────┤
  │        (repeated ENTRY_COUNT times)                        │
  └────────────────────────────────────────────────────────────┘
```

### 5.2 ASCII Byte Diagram

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| INDEX_MAGIC (0x4D49)  | VER   |         ENTRY_COUNT           |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     OFFSET[0] (uint64)                        |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    POSITION[0] (uint64)                       |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                     OFFSET[1] (uint64)                        |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    POSITION[1] (uint64)                       |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           ...                                 |
```

### 5.3 Index Constants

| Constant | Value | Description |
|---|---|---|
| `INDEX_MAGIC` | `0x4D49` (ASCII `"MI"`) | Identifies a MeshFlow index file |
| `INDEX_VERSION` | `0x01` | Index format version |
| `INDEX_HEADER_SIZE` | 7 bytes | MAGIC(2) + VERSION(1) + ENTRY_COUNT(4) |
| `INDEX_ENTRY_SIZE` | 16 bytes | OFFSET(8) + POSITION(8) |

### 5.4 Total File Size

```
total_index_size = INDEX_HEADER_SIZE + (ENTRY_COUNT × INDEX_ENTRY_SIZE)
                 = 7 + (N × 16) bytes
```

### 5.5 Entry Spacing

Index entries are added at configurable byte intervals (default: 4096 bytes).
An entry is recorded when:

1. It is the **first record** in the segment, OR
2. At least `interval_bytes` of record data have been written since the
   last index entry.

This produces a **sparse** index — not every offset is indexed.

### 5.6 Offset Lookup

To find a specific offset using the sparse index:

1. **Binary search** the index entries for the largest `OFFSET ≤ target_offset`.
2. Seek to the corresponding `POSITION` in the segment file.
3. **Scan forward** record by record until the target offset is found.

Time complexity: O(log N) for the binary search + O(K) for the forward scan,
where K is the number of records between the index entry and the target.

### 5.7 Index as Optimization

The index is **not** required for correctness:

- If the index file is missing or corrupt, it is silently rebuilt by scanning
  the segment file.
- The `load()` method returns `False` on any validation failure (wrong magic,
  wrong version, incorrect file size, non-increasing entries, truncated data,
  I/O error), signaling the caller to rebuild.

---

## 6. Recovery Procedure

### 6.1 Overview

On broker startup, the recovery system:

1. Discovers all partition directories under `data/<broker_id>/topics/`.
2. For each partition, discovers all segment files (sorted by base offset).
3. Recovers each segment individually.
4. Rebuilds in-memory state (next offset, record counts, sparse indexes).

### 6.2 Partition Discovery

```
data/<broker_id>/topics/
    <topic_name>/
        partition-<id>/         ← id parsed as integer
            segment-NNNNNNNNNN.log
            index-NNNNNNNNNN.idx
```

- Only directories matching `partition-<integer>` are considered.
- Segment files are discovered by finding `segment-*.log` files and
  parsing the base offset from the filename.
- Segments are processed in ascending base offset order.

### 6.3 Segment Recovery

For each segment, the recovery procedure performs:

```
position = 0
valid_records = []
last_valid_end = 0

expected_offset = segment_base_offset
while position < file_size:
    try:
        record, consumed = read_record_at(file, position)
        if record.offset != expected_offset:
            return DAMAGED
        valid_records.append((record, position))
        position += consumed
        last_valid_end = position
        expected_offset += 1
    except RecordTruncatedError:
        # Incomplete record at end — truncate the tail
        break
    except RecordCorruptionError:
        # Corruption at any position → mark DAMAGED
        return DAMAGED
```

### 6.4 Truncated Tail Handling

If the segment ends with an incomplete record (e.g., due to a crash during
a write), recovery **truncates** the file to the end of the last valid record:

```python
if last_valid_end < file_size:
    with open(log_path, "r+b") as f:
        f.truncate(last_valid_end)
```

This is safe because:
- Only the final incomplete frame is discarded; prior complete records remain
  intact and checksum-validated.
- All preceding records have valid CRC32 checksums.

An incomplete first record is also truncated to an empty segment. A complete
record with an invalid checksum or framing is not considered an interrupted
tail and is never truncated automatically. With `interval` and `os` durability
policies, an incomplete record may have been acknowledged before reaching stable
storage; this data-loss risk is part of those weaker policies.

### 6.5 Corruption Detection

| Scenario | Detection | Recovery Action |
|---|---|---|
| Invalid MAGIC bytes | `magic != 0x4D46` | `RecordCorruptionError` |
| Unknown VERSION | `version != 0x01` | `RecordCorruptionError` |
| CRC32 mismatch | `stored_crc != computed_crc` | `RecordCorruptionError` |
| RECORD_LENGTH too small | `length < 34` | `RecordCorruptionError` |
| RECORD_LENGTH too large | `length > max_record_bytes` | `RecordCorruptionError` |
| Incomplete frame header | `< 7 bytes available` | `RecordTruncatedError` |
| Incomplete body | `< RECORD_LENGTH bytes` | `RecordTruncatedError` |

### 6.6 Corruption and Offset Gaps

If any complete record is corrupt, its offset is unexpected, or segment base
offsets do not continue the partition's offset sequence, the entire partition
is marked as **DAMAGED** (`PartitionStatus.DAMAGED`). Recovery does not truncate
or skip such data. The broker will not serve this partition until manual
intervention.

### 6.7 Index Rebuilding

After recovering valid records, the sparse index is rebuilt from scratch:

1. Clear all existing index entries.
2. For each valid `(record, position)`, call `maybe_add_entry()` to
   conditionally add an index entry based on the configured byte interval.
3. Save the rebuilt index to disk.

### 6.8 State Restoration

After all segments for a partition are recovered:

- `next_offset` = last recovered record's offset + 1
- `total_records` = sum of records across all segments
- The last segment is reopened for appending (becomes the active segment)
- Partition status is set to `ACTIVE`

---

## 7. Limits and Safety

### 7.1 Maximum Record Size

| Limit | Default | Description |
|---|---|---|
| `max_record_bytes` | 16 MB (16 777 216 bytes) | Maximum allowed `RECORD_LENGTH` |

This limit is checked:
- **On write:** `record_size(record) > config.max_record_bytes` → `ValueError`
- **On read:** `RECORD_LENGTH > max_record_bytes` → `RecordCorruptionError`

The limit prevents a corrupt or malicious `RECORD_LENGTH` from causing the
reader to allocate unbounded memory.

### 7.2 Maximum Frame Size

```
max_frame_size = FRAME_HEADER_SIZE + max_record_bytes
               = 7 + 16,777,216
               = 16,777,223 bytes
```

### 7.3 Variable-Length Field Limits

| Field | Length Prefix | Max Size |
|---|---|---|
| `EVENT_ID` | uint16 | 65 535 bytes |
| `PARTITION_KEY` | uint16 | 65 535 bytes |
| `HEADERS` | uint32 | 4 294 967 295 bytes (bounded by max record size in practice) |
| `PAYLOAD` | uint32 | 4 294 967 295 bytes (bounded by max record size in practice) |

### 7.4 Length Validation Before Memory Allocation

Readers **must** validate `RECORD_LENGTH` before allocating a buffer:

```python
if record_length > max_record_bytes:
    raise RecordCorruptionError(...)
if record_length < MIN_BODY_SIZE:
    raise RecordCorruptionError(...)
```

This two-sided check ensures:
1. **Upper bound:** No allocation larger than `max_record_bytes` (default 16 MB).
2. **Lower bound:** The body is large enough to contain all fixed-size fields
   (34 bytes minimum).

### 7.5 Maximum Segment Size

| Limit | Default | Description |
|---|---|---|
| `max_bytes` | 64 MB (67 108 864 bytes) | Maximum segment file size before rotation |
| `max_age_ms` | 3 600 000 ms (1 hour) | Maximum segment age before rotation |

### 7.6 Offset Limits

Offsets are stored as uint64, giving a theoretical maximum of
18 446 744 073 709 551 615 (2⁶⁴ − 1) records per partition.

---

## Appendix A: Format Constants Quick Reference

| Constant | Value | Notes |
|---|---|---|
| `MAGIC` | `0x4D46` (`"MF"`) | Record frame magic |
| `FORMAT_VERSION` | `0x01` | Record format version |
| `FRAME_HEADER_SIZE` | 7 bytes | MAGIC + VERSION + RECORD_LENGTH |
| `CRC_SIZE` | 4 bytes | Trailing CRC32 |
| `MIN_BODY_SIZE` | 34 bytes | All fixed fields, no variable data |
| `DEFAULT_MAX_RECORD_BYTES` | 16 MB | Safety limit on RECORD_LENGTH |
| `INDEX_MAGIC` | `0x4D49` (`"MI"`) | Index file magic |
| `INDEX_VERSION` | `0x01` | Index format version |
| `INDEX_HEADER_SIZE` | 7 bytes | MAGIC + VERSION + ENTRY_COUNT |
| `INDEX_ENTRY_SIZE` | 16 bytes | OFFSET + POSITION |

## Appendix B: Python Struct Format Strings

| Field | Format | Description |
|---|---|---|
| `MAGIC` | 2 raw bytes | `b"\x4d\x46"` |
| `VERSION` | `>B` | Unsigned 8-bit |
| `RECORD_LENGTH` | `>I` | Unsigned 32-bit |
| `OFFSET` | `>Q` | Unsigned 64-bit |
| `TIMESTAMP_MS` | `>q` | Signed 64-bit |
| `EVENT_ID_LEN` | `>H` | Unsigned 16-bit |
| `SCHEMA_VERSION` | `>H` | Unsigned 16-bit |
| `PARTITION_KEY_LEN` | `>H` | Unsigned 16-bit |
| `HEADER_LEN` | `>I` | Unsigned 32-bit |
| `PAYLOAD_LEN` | `>I` | Unsigned 32-bit |
| `CRC32` | `>I` | Unsigned 32-bit |
| `INDEX OFFSET` | `>Q` | Unsigned 64-bit |
| `INDEX POSITION` | `>Q` | Unsigned 64-bit |
| `ENTRY_COUNT` | `>I` | Unsigned 32-bit |
