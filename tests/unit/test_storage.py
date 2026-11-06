"""Unit tests for meshflow.broker.storage."""

from __future__ import annotations

from pathlib import Path

import pytest

from meshflow.broker.storage import PartitionLog
from meshflow.common.config import BrokerConfig
from meshflow.common.types import Event, PartitionStatus

# ── Helpers ──────────────────────────────────────────────────────────


def _make_config(
    tmp_path: Path,
    segment_max_bytes: int = 64 * 1024 * 1024,
    max_record_bytes: int = 1 * 1024 * 1024,
) -> BrokerConfig:
    return BrokerConfig(
        broker_id="test-broker",
        data_dir=tmp_path,
        segment_max_bytes=segment_max_bytes,
        max_record_bytes=max_record_bytes,
        fsync_policy="every_write",
        index_interval_bytes=256,
    )


def _make_event(
    payload: bytes = b"test-payload",
    partition_key: str = "key-1",
    topic: str = "orders",
) -> Event:
    return Event.create(topic, partition_key, payload)


def _make_log(
    tmp_path: Path,
    topic: str = "orders",
    partition_id: int = 0,
    segment_max_bytes: int = 64 * 1024 * 1024,
    max_record_bytes: int = 1 * 1024 * 1024,
) -> PartitionLog:
    config = _make_config(
        tmp_path,
        segment_max_bytes=segment_max_bytes,
        max_record_bytes=max_record_bytes,
    )
    log = PartitionLog(topic=topic, partition_id=partition_id, config=config)
    log.initialize()
    return log


# ── Initialize new partition log ─────────────────────────────────────


class TestInitialize:
    """Tests for initializing a new partition log."""

    def test_initialize_creates_directory(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        assert log.directory.exists()

    def test_initialize_creates_first_segment(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        assert log.segment_count == 1

    def test_initialize_starts_at_offset_zero(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        assert log.next_offset == 0
        assert log.start_offset == 0

    def test_initialize_refuses_existing_segments(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        first = PartitionLog(topic="orders", partition_id=0, config=config)
        first.initialize()
        first.append(_make_event())
        first.close()

        reopened = PartitionLog(topic="orders", partition_id=0, config=config)
        with pytest.raises(RuntimeError, match="use recover"):
            reopened.initialize()


# ── Append and read ──────────────────────────────────────────────────


class TestAppendAndRead:
    """Tests for appending events and reading back by offset."""

    def test_append_and_read_single_event(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        event = _make_event(payload=b"hello")
        offset = log.append(event)
        assert offset == 0

        records = log.read(0, max_records=10)
        assert len(records) == 1
        assert records[0].offset == 0
        assert records[0].payload == b"hello"
        assert records[0].event_id == event.event_id

    def test_append_multiple_and_read_all(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        for i in range(10):
            log.append(_make_event(payload=f"data-{i}".encode()))

        records = log.read(0, max_records=100)
        assert len(records) == 10
        for i, rec in enumerate(records):
            assert rec.offset == i
            assert rec.payload == f"data-{i}".encode()

    def test_read_from_middle_offset(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        for i in range(10):
            log.append(_make_event(payload=f"data-{i}".encode()))

        records = log.read(5, max_records=100)
        assert len(records) == 5
        assert records[0].offset == 5


# ── Segment rotation ────────────────────────────────────────────────


class TestSegmentRotation:
    """Tests for segment rotation on size limit."""

    def test_rotation_triggers_on_size_exceeded(self, tmp_path: Path) -> None:
        """Use a small segment_max_bytes to force rotation."""
        log = _make_log(tmp_path, segment_max_bytes=500)
        for i in range(20):
            log.append(_make_event(payload=b"X" * 50))

        assert log.segment_count > 1

    def test_read_across_segment_boundaries(self, tmp_path: Path) -> None:
        """Ensure reads span multiple segments correctly."""
        log = _make_log(tmp_path, segment_max_bytes=500)
        for i in range(20):
            log.append(_make_event(payload=f"msg-{i}".encode()))

        records = log.read(0, max_records=100)
        assert len(records) == 20
        for i, rec in enumerate(records):
            assert rec.offset == i
            assert rec.payload == f"msg-{i}".encode()

    def test_rotation_triggers_on_age(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        assert log._active_segment is not None
        log.append(_make_event())
        log._active_segment._created_ms = 0

        log.append(_make_event())

        assert log.segment_count == 2


# ── next_offset ──────────────────────────────────────────────────────


class TestNextOffset:
    """Tests for next_offset correctness."""

    def test_next_offset_increments(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        assert log.next_offset == 0

        log.append(_make_event())
        assert log.next_offset == 1

        log.append(_make_event())
        assert log.next_offset == 2

        log.append(_make_event())
        assert log.next_offset == 3


# ── Append to non-ACTIVE partition ───────────────────────────────────


class TestNonActiveAppend:
    """Tests that appending to a non-ACTIVE partition raises RuntimeError."""

    def test_append_to_damaged_partition_raises(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        log._status = PartitionStatus.DAMAGED

        with pytest.raises(RuntimeError, match="Cannot append"):
            log.append(_make_event())

    def test_append_io_error_marks_partition_readonly(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        log = _make_log(tmp_path)
        assert log._active_segment is not None

        def fail_append(record) -> int:
            raise OSError("disk full")

        monkeypatch.setattr(log._active_segment, "append", fail_append)
        with pytest.raises(OSError, match="disk full"):
            log.append(_make_event())

        assert log.status == PartitionStatus.READONLY
        with pytest.raises(RuntimeError, match="status is readonly"):
            log.append(_make_event())

    def test_append_to_readonly_partition_raises(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        log._status = PartitionStatus.READONLY

        with pytest.raises(RuntimeError, match="Cannot append"):
            log.append(_make_event())


# ── Max record size ──────────────────────────────────────────────────


class TestMaxRecordSize:
    """Tests that oversized records are rejected."""

    def test_record_exceeding_max_bytes_raises(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path, max_record_bytes=200)
        large_payload = b"Y" * 500
        event = _make_event(payload=large_payload)

        with pytest.raises(ValueError, match="exceeds maximum"):
            log.append(event)


# ── close() and recover() round-trip ─────────────────────────────────


class TestCloseAndRecover:
    """Tests that data survives close → recover."""

    def test_close_and_recover_preserves_data(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        log = PartitionLog(topic="orders", partition_id=0, config=config)
        log.initialize()

        for i in range(5):
            log.append(_make_event(payload=f"msg-{i}".encode()))
        log.close()

        # Recover
        log2 = PartitionLog(topic="orders", partition_id=0, config=config)
        results = log2.recover()
        assert len(results) == 1
        assert results[0].status == "recovered"
        assert log2.status == PartitionStatus.ACTIVE

        records = log2.read(0, max_records=100)
        assert len(records) == 5
        for i, rec in enumerate(records):
            assert rec.offset == i
            assert rec.payload == f"msg-{i}".encode()

    def test_recover_continues_offset_sequence(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        log = PartitionLog(topic="orders", partition_id=0, config=config)
        log.initialize()
        for _ in range(3):
            log.append(_make_event())
        log.close()

        log2 = PartitionLog(topic="orders", partition_id=0, config=config)
        log2.recover()
        assert log2.next_offset == 3

        # Append more events after recovery
        log2.append(_make_event(payload=b"new-event"))
        assert log2.next_offset == 4

        records = log2.read(3, max_records=10)
        assert len(records) == 1
        assert records[0].payload == b"new-event"


# ── start_offset property ───────────────────────────────────────────


class TestStartOffset:
    """Tests for the start_offset property."""

    def test_start_offset_zero_on_init(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        assert log.start_offset == 0

    def test_start_offset_after_appends(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        for _ in range(5):
            log.append(_make_event())
        # start_offset should still be 0 (first segment base offset)
        assert log.start_offset == 0


# ── total_records ────────────────────────────────────────────────────


class TestTotalRecords:
    """Tests for total_records count."""

    def test_total_records_zero_on_init(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        assert log.total_records == 0

    def test_total_records_counts_correctly(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path)
        for _ in range(7):
            log.append(_make_event())
        assert log.total_records == 7

    def test_total_records_across_segments(self, tmp_path: Path) -> None:
        log = _make_log(tmp_path, segment_max_bytes=500)
        for _ in range(15):
            log.append(_make_event(payload=b"X" * 50))
        assert log.total_records == 15
        assert log.segment_count > 1
