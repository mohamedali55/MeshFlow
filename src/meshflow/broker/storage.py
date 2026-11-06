"""Partition log storage for the MeshFlow broker.

A PartitionLog manages all segments for a single partition. It handles:
- Appending records to the active segment.
- Rotating segments when size/age limits are reached.
- Reading records by offset across segments.
- Discovering and managing segment files on disk.

Directory layout:
    data/<broker_id>/topics/<topic>/partition-<id>/
        segment-0000000000.log
        index-0000000000.idx
        segment-0000000042.log
        index-0000000042.idx
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator
from pathlib import Path

from meshflow.broker.event_id_index import EventIdIndex
from meshflow.broker.segment import (
    LogSegment,
    SegmentRecoveryResult,
    parse_base_offset_from_filename,
    segment_filename,
)
from meshflow.broker.serialization import Record
from meshflow.common.config import BrokerConfig
from meshflow.common.types import Event, PartitionStatus

logger = logging.getLogger(__name__)


class PartitionLog:
    """Manages the append-only log for a single partition.

    The log consists of an ordered sequence of segments. Records are always
    appended to the active (most recent) segment. When the active segment
    reaches its size or age limit, it is closed and a new segment is created.
    """

    def __init__(
        self,
        topic: str,
        partition_id: int,
        config: BrokerConfig,
    ) -> None:
        """Initialize a partition log.

        Args:
            topic: Topic name.
            partition_id: Partition index.
            config: Broker configuration.
        """
        self._topic = topic
        self._partition_id = partition_id
        self._config = config
        self._directory = config.partition_dir(topic, partition_id)

        self._segments: list[LogSegment] = []
        self._active_segment: LogSegment | None = None
        self._next_offset: int = 0
        self._status: PartitionStatus = PartitionStatus.ACTIVE
        self._total_records: int = 0
        self._event_index = EventIdIndex(self._directory / "event-id-index.sqlite3")

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def partition_id(self) -> int:
        return self._partition_id

    @property
    def directory(self) -> Path:
        return self._directory

    @property
    def next_offset(self) -> int:
        return self._next_offset

    @property
    def status(self) -> PartitionStatus:
        return self._status

    @property
    def segment_count(self) -> int:
        return len(self._segments)

    @property
    def total_records(self) -> int:
        return self._total_records

    @property
    def start_offset(self) -> int:
        """First valid offset in the log."""
        if self._segments:
            return self._segments[0].base_offset
        return 0

    def initialize(self) -> None:
        """Initialize the partition log directory and create the first segment.

        Call this for new partitions. For existing partitions, use recover() instead.
        """
        if self._segments or any(self._directory.glob("segment-*.log")):
            raise RuntimeError(
                f"Partition {self._topic}/{self._partition_id} already has segments; "
                "use recover() instead of initialize()"
            )
        self._directory.mkdir(parents=True, exist_ok=True)
        self._event_index.rebuild(())
        self._create_new_segment(base_offset=0)

    def append(self, event: Event) -> int:
        """Append an event to the partition log.

        Creates a Record from the Event, assigns it the next offset, and
        writes it to the active segment. Rotates the segment if needed.

        Args:
            event: The event to append.

        Returns:
            The offset assigned to the event.

        Raises:
            RuntimeError: If the partition is not active.
            ValueError: If the record exceeds the maximum allowed size.
        """
        if self._status != PartitionStatus.ACTIVE:
            raise RuntimeError(
                f"Cannot append to partition {self._topic}/{self._partition_id}: "
                f"status is {self._status.value}"
            )

        if self._active_segment is None:
            self._create_new_segment(base_offset=self._next_offset)

        assert self._active_segment is not None

        # Check if rotation is needed
        if self._active_segment.needs_rotation() and self._active_segment.record_count > 0:
            self._rotate_segment()

        assert self._active_segment is not None

        record = Record.from_event(event, offset=self._next_offset)

        # Validate record size
        from meshflow.broker.serialization import record_size

        size = record_size(record)
        if size > self._config.max_record_bytes:
            raise ValueError(f"Record size {size} exceeds maximum {self._config.max_record_bytes}")

        offset = self._next_offset
        try:
            self._active_segment.append(record)
        except OSError:
            self._status = PartitionStatus.READONLY
            logger.exception(
                "Append failed for partition %s/%d; marking it read-only",
                self._topic,
                self._partition_id,
            )
            raise
        self._next_offset += 1
        self._total_records += 1
        try:
            self._event_index.add(event.event_id, offset)
        except (OSError, sqlite3.Error) as exc:
            self._status = PartitionStatus.READONLY
            logger.exception(
                "Event ID index update failed for partition %s/%d; marking it read-only",
                self._topic,
                self._partition_id,
            )
            raise RuntimeError("Event ID index update failed; recover the partition") from exc

        return offset

    def append_once(self, event: Event) -> tuple[int, bool]:
        """Append an event once per partition, returning its original offset on retry."""
        try:
            existing_offset = self._event_index.lookup(event.event_id)
        except (OSError, sqlite3.Error) as exc:
            self._status = PartitionStatus.READONLY
            logger.exception(
                "Event ID index lookup failed for partition %s/%d; marking it read-only",
                self._topic,
                self._partition_id,
            )
            raise RuntimeError("Event ID index lookup failed; recover the partition") from exc
        if existing_offset is not None:
            return existing_offset, False
        return self.append(event), True

    def read(
        self,
        start_offset: int,
        max_records: int = 100,
    ) -> list[Record]:
        """Read records starting from the given offset.

        Reads across segment boundaries if needed.

        Args:
            start_offset: First offset to read.
            max_records: Maximum number of records to return.

        Returns:
            A list of records starting at or after start_offset.
        """
        if self._status == PartitionStatus.DAMAGED:
            raise RuntimeError(
                f"Cannot read from damaged partition {self._topic}/{self._partition_id}"
            )

        if not self._segments:
            return []

        records: list[Record] = []
        remaining = max_records

        for segment in self._segments:
            if remaining <= 0:
                break
            if segment.next_offset <= start_offset:
                # This segment is entirely before our target
                continue

            segment_records = segment.read_from_offset(
                start_offset,
                max_records=remaining,
                max_record_bytes=self._config.max_record_bytes,
            )
            records.extend(segment_records)
            remaining -= len(segment_records)

            # Update start_offset for cross-segment reads
            if segment_records:
                start_offset = segment_records[-1].offset + 1

        return records

    def recover(self) -> list[SegmentRecoveryResult]:
        """Recover the partition log from disk.

        Discovers all segment files, validates them, rebuilds indexes,
        and recovers the next offset. Handles truncated tails and reports
        corruption.

        Returns:
            A list of SegmentRecoveryResult for each recovered segment.
        """
        results: list[SegmentRecoveryResult] = []
        self._segments.clear()
        self._active_segment = None
        self._next_offset = 0
        self._total_records = 0

        if not self._directory.exists():
            logger.info(
                "Partition directory does not exist: %s/%d — treating as new",
                self._topic,
                self._partition_id,
            )
            self._status = PartitionStatus.ACTIVE
            return results
        self._event_index.rebuild(())

        # Discover segment files
        base_offsets = self._discover_segments()
        if not base_offsets:
            logger.info(
                "No segments found for %s/%d",
                self._topic,
                self._partition_id,
            )
            self._status = PartitionStatus.ACTIVE
            return results

        # Recover each segment in order
        for base_offset in base_offsets:
            segment = LogSegment(
                directory=self._directory,
                base_offset=base_offset,
                max_bytes=self._config.segment_max_bytes,
                max_age_ms=self._config.segment_max_age_ms,
                index_interval_bytes=self._config.index_interval_bytes,
                fsync_policy=self._config.fsync_policy,
                fsync_interval_ms=self._config.fsync_interval_ms,
            )

            if base_offset != self._next_offset:
                message = (
                    f"Segment base offset {base_offset} does not continue "
                    f"partition offset {self._next_offset}"
                )
                logger.error("Partition %s/%d: %s", self._topic, self._partition_id, message)
                self._status = PartitionStatus.DAMAGED
                results.append(
                    SegmentRecoveryResult(
                        base_offset=base_offset,
                        log_path=self._directory / segment_filename(base_offset),
                        status="damaged",
                        corruption_offset=0,
                        error=message,
                    )
                )
                return results

            result = segment.recover(max_record_bytes=self._config.max_record_bytes)
            results.append(result)

            if result.status == "damaged":
                logger.error(
                    "Segment %s is damaged at offset %s: %s",
                    result.log_path,
                    result.corruption_offset,
                    result.error,
                )
                self._status = PartitionStatus.DAMAGED
                return results

            self._segments.append(segment)
            if result.records_recovered > 0:
                self._next_offset = result.next_offset
                self._total_records += result.records_recovered

                if result.truncated_bytes > 0:
                    logger.warning(
                        "Segment %s: truncated %d bytes (incomplete tail)",
                        result.log_path,
                        result.truncated_bytes,
                    )
            else:
                self._next_offset = result.next_offset

        # The last segment becomes the active segment (reopened for appending)
        if self._segments:
            self._event_index.rebuild(self._iter_records())
            self._active_segment = self._segments[-1]
            self._active_segment.open()
            logger.info(
                "Recovered partition %s/%d: %d segments, %d records, next_offset=%d",
                self._topic,
                self._partition_id,
                len(self._segments),
                self._total_records,
                self._next_offset,
            )
        self._status = PartitionStatus.ACTIVE
        return results

    def close(self) -> None:
        """Close the partition log and all segments."""
        for segment in self._segments:
            if not segment.is_closed:
                segment.close()
        self._active_segment = None
        self._event_index.close()

    def flush(self) -> None:
        """Flush the active segment to disk."""
        if self._active_segment is not None:
            self._active_segment.flush()

    # ── Private methods ───────────────────────────────────────────────

    def _create_new_segment(self, base_offset: int) -> LogSegment:
        """Create and activate a new segment."""
        segment = LogSegment(
            directory=self._directory,
            base_offset=base_offset,
            max_bytes=self._config.segment_max_bytes,
            max_age_ms=self._config.segment_max_age_ms,
            index_interval_bytes=self._config.index_interval_bytes,
            fsync_policy=self._config.fsync_policy,
            fsync_interval_ms=self._config.fsync_interval_ms,
        )
        segment.open()
        self._segments.append(segment)
        self._active_segment = segment
        return segment

    def _rotate_segment(self) -> None:
        """Close the active segment and create a new one."""
        if self._active_segment is not None:
            self._active_segment.close()
            logger.info(
                "Rotated segment for %s/%d: closed segment at base_offset=%d "
                "(%d records, %d bytes), new segment at base_offset=%d",
                self._topic,
                self._partition_id,
                self._active_segment.base_offset,
                self._active_segment.record_count,
                self._active_segment.byte_size,
                self._next_offset,
            )
        self._create_new_segment(base_offset=self._next_offset)

    def _discover_segments(self) -> list[int]:
        """Discover and return sorted base offsets of segment files on disk."""
        base_offsets: set[int] = set()
        for path in self._directory.iterdir():
            if path.suffix == ".log" and path.name.startswith("segment-"):
                offset = parse_base_offset_from_filename(path.name)
                if offset is not None:
                    base_offsets.add(offset)
        return sorted(base_offsets)

    def _iter_records(self) -> Iterator[Record]:
        for segment in self._segments:
            offset = segment.base_offset
            while offset < segment.next_offset:
                records = segment.read_from_offset(
                    offset,
                    max_records=1024,
                    max_record_bytes=self._config.max_record_bytes,
                )
                if not records:
                    raise RuntimeError(
                        f"Could not rebuild event ID index for {self._topic}/"
                        f"{self._partition_id} at offset {offset}"
                    )
                yield from records
                offset = records[-1].offset + 1
