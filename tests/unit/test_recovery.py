"""Unit tests for meshflow.broker.recovery."""

from __future__ import annotations

from pathlib import Path

import pytest

from meshflow.broker.recovery import BrokerRecovery
from meshflow.broker.serialization import Record, serialize_record
from meshflow.broker.storage import PartitionLog
from meshflow.common.config import BrokerConfig
from meshflow.common.types import Event, PartitionStatus

# ── Helpers ──────────────────────────────────────────────────────────


def _make_config(tmp_path: Path, **overrides: object) -> BrokerConfig:
    defaults: dict[str, object] = dict(
        broker_id="test-broker",
        data_dir=tmp_path,
        segment_max_bytes=64 * 1024 * 1024,
        max_record_bytes=1 * 1024 * 1024,
        fsync_policy="every_write",
        index_interval_bytes=256,
    )
    defaults.update(overrides)
    return BrokerConfig(**defaults)  # type: ignore[arg-type]


def _make_event(payload: bytes = b"test-payload") -> Event:
    return Event.create("orders", "key-1", payload)


def _populate_partition(
    tmp_path: Path,
    config: BrokerConfig,
    topic: str,
    partition_id: int,
    event_count: int,
) -> None:
    """Create and populate a partition log with events, then close it."""
    log = PartitionLog(topic=topic, partition_id=partition_id, config=config)
    log.initialize()
    for i in range(event_count):
        log.append(_make_event(payload=f"data-{i}".encode()))
    log.close()


# ── Recover partition after clean close ──────────────────────────────


class TestRecoverCleanPartition:
    """Tests for recovering a partition that was closed cleanly."""

    def test_recover_after_clean_close(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        _populate_partition(tmp_path, config, "orders", 0, 5)

        log = PartitionLog(topic="orders", partition_id=0, config=config)
        results = log.recover()

        assert log.status == PartitionStatus.ACTIVE
        assert results[0].status == "recovered"
        assert log.total_records == 5
        assert log.next_offset == 5

        records = log.read(0, max_records=100)
        assert len(records) == 5


# ── Recover partition with truncated tail ────────────────────────────


class TestRecoverTruncatedTail:
    """Tests for recovering a partition with an incomplete record at the end."""

    def test_recover_with_truncated_tail(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        _populate_partition(tmp_path, config, "orders", 0, 3)

        # Append garbage to simulate a truncated write
        partition_dir = config.partition_dir("orders", 0)
        log_file = partition_dir / "segment-0000000000.log"
        with open(log_file, "ab") as f:
            f.write(b"\x4d\x46\x01\x00\x00\x00\x40")  # partial MF header

        log = PartitionLog(topic="orders", partition_id=0, config=config)
        results = log.recover()

        assert log.status == PartitionStatus.ACTIVE
        assert results[0].truncated_bytes > 0
        assert log.total_records == 3
        assert log.next_offset == 3


class TestRecoverCorruption:
    def test_corruption_after_valid_records_marks_partition_damaged(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        _populate_partition(tmp_path, config, "orders", 0, 3)
        log_file = config.partition_dir("orders", 0) / "segment-0000000000.log"
        original = bytearray(log_file.read_bytes())
        first_record_size = 7 + int.from_bytes(original[3:7], "big")
        original[first_record_size + 17] ^= 0xFF
        log_file.write_bytes(original)

        log = PartitionLog(topic="orders", partition_id=0, config=config)
        results = log.recover()

        assert log.status == PartitionStatus.DAMAGED
        assert results[0].status == "damaged"
        assert log_file.read_bytes() == original
        with pytest.raises(RuntimeError, match="damaged partition"):
            log.read(0)

    def test_partition_recovery_rejects_segment_offset_gap(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        partition_dir = config.partition_dir("orders", 0)
        partition_dir.mkdir(parents=True, exist_ok=True)
        first = Record(
            offset=0,
            timestamp_ms=1,
            event_id="evt-0",
            schema_version=1,
            partition_key="key",
            headers={},
            payload=b"first",
        )
        later = Record(
            offset=2,
            timestamp_ms=2,
            event_id="evt-2",
            schema_version=1,
            partition_key="key",
            headers={},
            payload=b"later",
        )
        (partition_dir / "segment-0000000000.log").write_bytes(serialize_record(first))
        (partition_dir / "segment-0000000002.log").write_bytes(serialize_record(later))

        log = PartitionLog(topic="orders", partition_id=0, config=config)
        results = log.recover()

        assert log.status == PartitionStatus.DAMAGED
        assert results[-1].error is not None
        assert "does not continue" in results[-1].error

    def test_recovery_rebuilds_sparse_index_at_byte_intervals(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path, index_interval_bytes=1)
        _populate_partition(tmp_path, config, "orders", 0, 5)
        index_file = config.partition_dir("orders", 0) / "index-0000000000.idx"
        index_file.unlink()

        log = PartitionLog(topic="orders", partition_id=0, config=config)
        log.recover()

        assert log._active_segment is not None
        assert log._active_segment.index.entry_count == 5


# ── Recover empty partition directory ────────────────────────────────


class TestRecoverEmptyDirectory:
    """Tests for recovering an empty partition directory."""

    def test_recover_empty_partition_directory(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        # Create the partition directory but don't add any segments
        partition_dir = config.partition_dir("orders", 0)
        partition_dir.mkdir(parents=True, exist_ok=True)

        log = PartitionLog(topic="orders", partition_id=0, config=config)
        results = log.recover()

        assert log.status == PartitionStatus.ACTIVE
        assert log.total_records == 0
        assert log.next_offset == 0
        assert results == []


# ── Recover non-existent directory ───────────────────────────────────


class TestRecoverNonExistentDirectory:
    """Tests for recovering when partition directory doesn't exist."""

    def test_recover_non_existent_directory(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        log = PartitionLog(topic="orders", partition_id=0, config=config)
        results = log.recover()

        assert log.status == PartitionStatus.ACTIVE
        assert log.total_records == 0
        assert log.next_offset == 0
        assert results == []


# ── BrokerRecovery discovers multiple topics/partitions ──────────────


class TestBrokerRecoveryDiscovery:
    """Tests for BrokerRecovery discovering and recovering partitions."""

    def test_discovers_multiple_topics_and_partitions(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)

        # Create partitions across two topics
        _populate_partition(tmp_path, config, "orders", 0, 3)
        _populate_partition(tmp_path, config, "orders", 1, 2)
        _populate_partition(tmp_path, config, "events", 0, 4)

        recovery = BrokerRecovery(config)
        result = recovery.recover()

        assert result.total_partitions == 3
        assert result.healthy_partitions == 3
        assert result.damaged_partitions == 0
        assert result.is_healthy

        # All partitions should be accessible
        assert recovery.get_partition_log("orders", 0) is not None
        assert recovery.get_partition_log("orders", 1) is not None
        assert recovery.get_partition_log("events", 0) is not None

    def test_discovers_no_topics_on_clean_start(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        recovery = BrokerRecovery(config)
        result = recovery.recover()

        assert result.total_partitions == 0
        assert result.is_healthy


# ── BrokerRecovery reports damaged partition ─────────────────────────


class TestBrokerRecoveryDamaged:
    """Tests for BrokerRecovery handling damaged partitions."""

    def test_reports_damaged_partition(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)

        # Create a healthy partition
        _populate_partition(tmp_path, config, "orders", 0, 3)

        # Create a damaged partition by writing garbage as segment data
        damaged_dir = config.partition_dir("orders", 1)
        damaged_dir.mkdir(parents=True, exist_ok=True)
        (damaged_dir / "segment-0000000000.log").write_bytes(b"\xff\xff\xff\xff" * 20)

        recovery = BrokerRecovery(config)
        result = recovery.recover()

        assert result.total_partitions == 2
        assert result.healthy_partitions == 1
        assert result.damaged_partitions == 1
        assert not result.is_healthy


# ── get_partition_log returns None for damaged ───────────────────────


class TestGetPartitionLog:
    """Tests for get_partition_log with damaged and healthy partitions."""

    def test_returns_none_for_damaged_partition(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)

        # Create a damaged partition
        damaged_dir = config.partition_dir("orders", 0)
        damaged_dir.mkdir(parents=True, exist_ok=True)
        (damaged_dir / "segment-0000000000.log").write_bytes(b"\xff\xff\xff\xff" * 20)

        recovery = BrokerRecovery(config)
        recovery.recover()

        assert recovery.get_partition_log("orders", 0) is None

    def test_returns_none_for_nonexistent_partition(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        recovery = BrokerRecovery(config)
        recovery.recover()

        assert recovery.get_partition_log("nonexistent", 99) is None


# ── Recovered partition log is appendable ────────────────────────────


class TestRecoveredPartitionAppendable:
    """Tests that a recovered partition can accept new events."""

    def test_append_after_recovery(self, tmp_path: Path) -> None:
        config = _make_config(tmp_path)
        _populate_partition(tmp_path, config, "orders", 0, 3)

        recovery = BrokerRecovery(config)
        recovery.recover()

        log = recovery.get_partition_log("orders", 0)
        assert log is not None
        assert log.status == PartitionStatus.ACTIVE
        assert log.next_offset == 3

        # Append new events
        log.append(_make_event(payload=b"new-1"))
        log.append(_make_event(payload=b"new-2"))

        assert log.next_offset == 5
        assert log.total_records == 5

        # Read back all records including new ones
        records = log.read(0, max_records=100)
        assert len(records) == 5
        assert records[3].payload == b"new-1"
        assert records[4].payload == b"new-2"
