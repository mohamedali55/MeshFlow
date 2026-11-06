"""Log segment management for the MeshFlow storage engine.

A segment is an append-only file that stores a contiguous range of records
for a single partition. Segments are rotated when they reach a configured
size or age limit.

Layout:
    data/<broker_id>/topics/<topic>/partition-<id>/
        segment-0000000000.log   ← records for offsets 0..N
        index-0000000000.idx     ← sparse index for the segment
        segment-0000000042.log   ← records for offsets 42..M
        index-0000000042.idx
        ...

Segments are named by their base offset (the offset of the first record
they contain).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from meshflow.broker.index import SparseIndex
from meshflow.broker.serialization import (
    Record,
    RecordCorruptionError,
    RecordTruncatedError,
    read_record_at,
    record_size,
    serialize_record,
)


def segment_filename(base_offset: int) -> str:
    """Generate the segment log filename for a base offset."""
    return f"segment-{base_offset:010d}.log"


def index_filename(base_offset: int) -> str:
    """Generate the index filename for a base offset."""
    return f"index-{base_offset:010d}.idx"


def parse_base_offset_from_filename(filename: str) -> int | None:
    """Extract the base offset from a segment or index filename.

    Returns None if the filename doesn't match the expected pattern.
    """
    name = Path(filename).stem
    for prefix in ("segment-", "index-"):
        if name.startswith(prefix):
            try:
                return int(name[len(prefix) :])
            except ValueError:
                return None
    return None


@dataclass
class SegmentInfo:
    """Metadata about a segment."""

    base_offset: int
    next_offset: int
    byte_size: int
    record_count: int
    created_ms: int
    is_closed: bool


class LogSegment:
    """A single append-only log segment file with its sparse index.

    Records are appended to the segment sequentially. The segment tracks
    its base offset (first record), next offset, byte size, and creation time.
    """

    def __init__(
        self,
        directory: Path,
        base_offset: int,
        *,
        max_bytes: int = 64 * 1024 * 1024,
        max_age_ms: int = 3600 * 1000,
        index_interval_bytes: int = 4096,
        fsync_policy: str = "every_write",
        fsync_interval_ms: int = 1000,
    ) -> None:
        """Initialize a log segment.

        Args:
            directory: Partition directory containing segment files.
            base_offset: Offset of the first record in this segment.
            max_bytes: Maximum segment size before rotation is needed.
            max_age_ms: Maximum segment age before rotation is needed.
            index_interval_bytes: Bytes between sparse index entries.
            fsync_policy: "every_write", "interval", or "os".
        """
        self._directory = directory
        self._base_offset = base_offset
        self._max_bytes = max_bytes
        self._max_age_ms = max_age_ms
        self._fsync_policy = fsync_policy
        self._fsync_interval_ms = fsync_interval_ms

        self._log_path = directory / segment_filename(base_offset)
        self._index = SparseIndex(
            directory / index_filename(base_offset),
            interval_bytes=index_interval_bytes,
        )

        self._next_offset = base_offset
        self._byte_size = 0
        self._record_count = 0
        self._created_ms = int(time.time() * 1000)
        self._closed = False
        self._file: BinaryIO | None = None
        self._last_fsync_ms = self._created_ms

    @property
    def base_offset(self) -> int:
        return self._base_offset

    @property
    def next_offset(self) -> int:
        return self._next_offset

    @property
    def byte_size(self) -> int:
        return self._byte_size

    @property
    def record_count(self) -> int:
        return self._record_count

    @property
    def log_path(self) -> Path:
        return self._log_path

    @property
    def index(self) -> SparseIndex:
        return self._index

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def info(self) -> SegmentInfo:
        return SegmentInfo(
            base_offset=self._base_offset,
            next_offset=self._next_offset,
            byte_size=self._byte_size,
            record_count=self._record_count,
            created_ms=self._created_ms,
            is_closed=self._closed,
        )

    def needs_rotation(self) -> bool:
        """Check if this segment should be rotated based on size or age."""
        if self._closed:
            return True
        if self._byte_size >= self._max_bytes:
            return True
        age_ms = int(time.time() * 1000) - self._created_ms
        if age_ms >= self._max_age_ms:
            return True
        return False

    def open(self) -> None:
        """Open the segment file for appending. Creates the file if needed."""
        if self._file is not None:
            return
        self._directory.mkdir(parents=True, exist_ok=True)
        self._file = open(self._log_path, "ab")  # noqa: SIM115
        self._byte_size = self._file.tell()

    def append(self, record: Record) -> int:
        """Append a record to the segment.

        The record's offset field is set to the segment's next_offset.

        Args:
            record: The record to append.

        Returns:
            The byte position where the record was written.

        Raises:
            RuntimeError: If the segment is closed.
            OSError: On write failures.
        """
        if self._closed:
            raise RuntimeError("Cannot append to a closed segment")
        if self._file is None:
            self.open()

        # Assign offset
        record.offset = self._next_offset
        data = serialize_record(record)
        position = self._byte_size

        assert self._file is not None
        self._file.write(data)

        now_ms = int(time.time() * 1000)
        if self._fsync_policy == "every_write" or (
            self._fsync_policy == "interval"
            and now_ms - self._last_fsync_ms >= self._fsync_interval_ms
        ):
            self._file.flush()
            os.fsync(self._file.fileno())
            self._last_fsync_ms = now_ms

        # Update index
        self._index.maybe_add_entry(record.offset, position, len(data))

        self._next_offset += 1
        self._byte_size += len(data)
        self._record_count += 1

        return position

    def read_at(self, position: int, max_record_bytes: int = 16 * 1024 * 1024) -> Record:
        """Read a single record at the given byte position.

        Args:
            position: Byte offset into the segment file.
            max_record_bytes: Maximum record size for safety validation.

        Returns:
            The deserialized Record.
        """
        with open(self._log_path, "rb") as f:
            record, _ = read_record_at(f, position, max_record_bytes)
        return record

    def read_from_offset(
        self,
        target_offset: int,
        max_records: int = 100,
        max_record_bytes: int = 16 * 1024 * 1024,
    ) -> list[Record]:
        """Read records starting from the given offset.

        Uses the sparse index to find a nearby position, then scans forward.

        Args:
            target_offset: The offset to start reading from.
            max_records: Maximum number of records to return.
            max_record_bytes: Maximum record size for safety validation.

        Returns:
            A list of records starting at or after target_offset.
        """
        if target_offset >= self._next_offset:
            return []
        if target_offset < self._base_offset:
            target_offset = self._base_offset

        # Use sparse index to find a starting position
        entry = self._index.lookup(target_offset)
        start_position = entry.position if entry else 0

        records: list[Record] = []
        with open(self._log_path, "rb") as f:
            position = start_position
            while len(records) < max_records and position < self._byte_size:
                record, consumed = read_record_at(f, position, max_record_bytes)

                if record.offset >= target_offset:
                    records.append(record)
                position += consumed

        return records

    def flush(self) -> None:
        """Flush and fsync the segment file."""
        if self._file is not None:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._last_fsync_ms = int(time.time() * 1000)

    def close(self) -> None:
        """Close the segment file and save the index."""
        self._closed = True
        if self._file is not None:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()
            self._file = None
        self._index.save()

    def scan_all_records(
        self, max_record_bytes: int = 16 * 1024 * 1024
    ) -> list[tuple[Record, int]]:
        """Scan all records in the segment for index rebuilding.

        Returns:
            List of (Record, byte_position) tuples for all valid records.

        Raises:
            RecordCorruptionError: If a record is corrupt.
            RecordTruncatedError: If a record is incomplete.
        """
        records: list[tuple[Record, int]] = []
        if not self._log_path.exists():
            return records

        with open(self._log_path, "rb") as f:
            position = 0
            file_size = self._log_path.stat().st_size
            while position < file_size:
                record, consumed = read_record_at(f, position, max_record_bytes)
                records.append((record, position))
                position += consumed

        return records

    def rebuild_index(self, max_record_bytes: int = 16 * 1024 * 1024) -> int:
        """Rebuild the sparse index by scanning the segment.

        Returns:
            The number of valid records found.
        """
        self._index.clear()
        records = self.scan_all_records(max_record_bytes)
        for record, position in records:
            self._index.maybe_add_entry(record.offset, position, record_size(record))
        self._index.save()
        return len(records)

    def recover(self, max_record_bytes: int = 16 * 1024 * 1024) -> SegmentRecoveryResult:
        """Recover the segment: validate records, detect corruption, rebuild state.

        Scans from the beginning, validating each record. If a truncated
        tail is found (incomplete final record from an interrupted append),
        it is truncated. Mid-segment corruption stops recovery.

        Returns:
            A SegmentRecoveryResult with recovery details.
        """
        result = SegmentRecoveryResult(
            base_offset=self._base_offset,
            log_path=self._log_path,
        )

        if not self._log_path.exists():
            result.status = "empty"
            result.next_offset = self._base_offset
            return result

        file_size = self._log_path.stat().st_size
        if file_size == 0:
            result.status = "empty"
            result.next_offset = self._base_offset
            return result

        valid_records: list[tuple[Record, int]] = []
        last_valid_end = 0
        expected_offset = self._base_offset

        with open(self._log_path, "rb") as f:
            position = 0
            while position < file_size:
                try:
                    record, consumed = read_record_at(f, position, max_record_bytes)
                    if record.offset != expected_offset:
                        result.status = "damaged"
                        result.corruption_offset = position
                        result.error = (
                            f"Unexpected record offset {record.offset}; expected {expected_offset}"
                        )
                        return result
                    valid_records.append((record, position))
                    position += consumed
                    last_valid_end = position
                    expected_offset += 1
                except RecordTruncatedError:
                    # Truncated tail — this is recoverable
                    result.truncated_bytes = file_size - position
                    break
                except RecordCorruptionError as e:
                    result.status = "damaged"
                    result.corruption_offset = position
                    result.error = str(e)
                    return result

        # A partial frame is safe to discard, including when it is the only data.
        if last_valid_end < file_size:
            result.truncated_bytes = file_size - last_valid_end
            with open(self._log_path, "r+b") as f:
                f.truncate(last_valid_end)

        # Rebuild state
        if valid_records:
            self._record_count = len(valid_records)
            last_record, _ = valid_records[-1]
            self._next_offset = last_record.offset + 1
            self._byte_size = last_valid_end

            # Rebuild index
            self._index.clear()
            for record, position in valid_records:
                self._index.maybe_add_entry(record.offset, position, record_size(record))
            self._index.save()

            result.records_recovered = len(valid_records)
            result.next_offset = self._next_offset
            result.status = "recovered"
        else:
            self._next_offset = self._base_offset
            self._byte_size = 0
            self._record_count = 0
            result.next_offset = self._base_offset
            result.status = "empty"

        return result


@dataclass
class SegmentRecoveryResult:
    """Result of recovering a single segment."""

    base_offset: int
    log_path: Path
    status: str = "unknown"  # "recovered", "empty", "damaged"
    records_recovered: int = 0
    next_offset: int = 0
    truncated_bytes: int = 0
    corruption_offset: int | None = None
    error: str | None = None

    @property
    def is_healthy(self) -> bool:
        return self.status in ("recovered", "empty")
