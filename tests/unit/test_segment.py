"""Unit tests for meshflow.broker.segment."""

from __future__ import annotations

from pathlib import Path

import pytest

from meshflow.broker.segment import LogSegment, SegmentInfo
from meshflow.broker.serialization import (
    Record,
    serialize_record,
)

# ── Helpers ──────────────────────────────────────────────────────────


def _make_record(
    offset: int = 0,
    event_id: str = "evt-1",
    payload: bytes = b"hello",
) -> Record:
    return Record(
        offset=offset,
        timestamp_ms=1700000000000,
        event_id=event_id,
        schema_version=1,
        partition_key="key-1",
        headers={},
        payload=payload,
    )


def _make_segment(
    directory: Path,
    base_offset: int = 0,
    max_bytes: int = 64 * 1024 * 1024,
    index_interval_bytes: int = 4096,
) -> LogSegment:
    seg = LogSegment(
        directory=directory,
        base_offset=base_offset,
        max_bytes=max_bytes,
        index_interval_bytes=index_interval_bytes,
        fsync_policy="every_write",
    )
    seg.open()
    return seg


# ── Append and read back ────────────────────────────────────────────


class TestAppendAndRead:
    """Tests for appending records and reading them back."""

    def test_append_and_read_at(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path)
        rec = _make_record()
        position = seg.append(rec)

        read_back = seg.read_at(position)
        assert read_back.offset == 0
        assert read_back.event_id == "evt-1"
        assert read_back.payload == b"hello"

    def test_append_multiple_and_read(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path)
        positions = []
        for i in range(5):
            rec = _make_record(event_id=f"evt-{i}", payload=f"data-{i}".encode())
            positions.append(seg.append(rec))

        for i, pos in enumerate(positions):
            read_back = seg.read_at(pos)
            assert read_back.offset == i
            assert read_back.event_id == f"evt-{i}"

    def test_append_assigns_sequential_offsets(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path, base_offset=10)
        for i in range(3):
            seg.append(_make_record())

        assert seg.next_offset == 13
        assert seg.record_count == 3


# ── read_from_offset with sparse index ──────────────────────────────


class TestReadFromOffset:
    """Tests for reading from a target offset using the sparse index."""

    def test_read_from_offset_returns_records(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path, index_interval_bytes=1)
        for i in range(10):
            seg.append(_make_record(event_id=f"evt-{i}"))

        records = seg.read_from_offset(5)
        assert len(records) == 5
        assert records[0].offset == 5
        assert records[-1].offset == 9

    def test_read_from_offset_respects_max_records(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path, index_interval_bytes=1)
        for i in range(10):
            seg.append(_make_record(event_id=f"evt-{i}"))

        records = seg.read_from_offset(0, max_records=3)
        assert len(records) == 3

    def test_read_from_offset_beyond_next_returns_empty(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path)
        seg.append(_make_record())

        records = seg.read_from_offset(100)
        assert records == []


# ── needs_rotation ───────────────────────────────────────────────────


class TestNeedsRotation:
    """Tests for segment rotation detection."""

    def test_needs_rotation_when_exceeding_max_bytes(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path, max_bytes=100)
        # Append records until byte size exceeds the limit
        for _ in range(20):
            seg.append(_make_record(payload=b"X" * 50))

        assert seg.needs_rotation() is True

    def test_no_rotation_needed_when_small(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path, max_bytes=10 * 1024 * 1024)
        seg.append(_make_record())
        assert seg.needs_rotation() is False


# ── scan_all_records ─────────────────────────────────────────────────


class TestScanAllRecords:
    """Tests for scanning all records in a segment."""

    def test_scan_all_records_returns_all(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path)
        for i in range(5):
            seg.append(_make_record(event_id=f"evt-{i}"))

        results = seg.scan_all_records()
        assert len(results) == 5
        for i, (record, position) in enumerate(results):
            assert record.offset == i
            assert record.event_id == f"evt-{i}"
            assert isinstance(position, int)

    def test_scan_empty_segment_returns_empty(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path)
        results = seg.scan_all_records()
        assert results == []


# ── rebuild_index ────────────────────────────────────────────────────


class TestRebuildIndex:
    """Tests for rebuilding the sparse index."""

    def test_rebuild_index_creates_correct_entries(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path, index_interval_bytes=1)
        for i in range(5):
            seg.append(_make_record(event_id=f"evt-{i}"))

        # Clear and rebuild
        seg.index.clear()
        assert seg.index.entry_count == 0

        count = seg.rebuild_index()
        assert count == 5
        assert seg.index.entry_count == 5

        # Verify lookups work after rebuild
        entry = seg.index.lookup(0)
        assert entry is not None
        assert entry.offset == 0
        assert entry.position == 0


# ── recover() ────────────────────────────────────────────────────────


class TestSegmentRecover:
    """Tests for segment recovery."""

    def test_recover_clean_segment(self, tmp_path: Path) -> None:
        """Recover a segment with no corruption."""
        seg = _make_segment(tmp_path)
        for i in range(5):
            seg.append(_make_record(event_id=f"evt-{i}"))
        seg.close()

        # Create a new segment instance and recover
        seg2 = LogSegment(directory=tmp_path, base_offset=0, fsync_policy="every_write")
        result = seg2.recover()

        assert result.status == "recovered"
        assert result.records_recovered == 5
        assert result.next_offset == 5
        assert result.truncated_bytes == 0
        assert result.is_healthy

    def test_recover_truncated_tail(self, tmp_path: Path) -> None:
        """Recover with a partial record at the end (simulating crash)."""
        seg = _make_segment(tmp_path)
        for i in range(3):
            seg.append(_make_record(event_id=f"evt-{i}"))
        seg.close()

        # Append partial garbage at the end to simulate truncated write
        log_file = tmp_path / "segment-0000000000.log"
        original_size = log_file.stat().st_size
        with open(log_file, "ab") as f:
            f.write(b"\x4d\x46\x01\x00\x00\x00\x40")  # valid header start, incomplete body

        seg2 = LogSegment(directory=tmp_path, base_offset=0, fsync_policy="every_write")
        result = seg2.recover()

        assert result.status == "recovered"
        assert result.records_recovered == 3
        assert result.next_offset == 3
        assert result.truncated_bytes > 0
        # The file should be truncated back to the valid end
        assert log_file.stat().st_size == original_size

    def test_recover_corruption_at_start(self, tmp_path: Path) -> None:
        """Corruption at byte 0 → segment is damaged."""
        log_file = tmp_path / "segment-0000000000.log"
        tmp_path.mkdir(parents=True, exist_ok=True)
        log_file.write_bytes(b"\xff\xff\xff\xff" * 10)

        seg = LogSegment(directory=tmp_path, base_offset=0, fsync_policy="every_write")
        result = seg.recover()

        assert result.status == "damaged"
        assert not result.is_healthy

    def test_recover_corruption_after_valid_record_does_not_truncate(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path)
        for i in range(3):
            seg.append(_make_record(event_id=f"evt-{i}"))
        seg.close()

        log_file = tmp_path / "segment-0000000000.log"
        original = bytearray(log_file.read_bytes())
        first_record_size = 7 + int.from_bytes(original[3:7], "big")
        original[first_record_size + 17] ^= 0xFF
        log_file.write_bytes(original)

        recovered = LogSegment(directory=tmp_path, base_offset=0)
        result = recovered.recover()

        assert result.status == "damaged"
        assert result.corruption_offset == first_record_size
        assert log_file.read_bytes() == original

    def test_recover_truncated_first_record_truncates_to_empty(self, tmp_path: Path) -> None:
        log_file = tmp_path / "segment-0000000000.log"
        log_file.write_bytes(b"\x4d\x46\x01\x00\x00\x00\x40")

        seg = LogSegment(directory=tmp_path, base_offset=0)
        result = seg.recover()

        assert result.status == "empty"
        assert result.truncated_bytes == 7
        assert result.next_offset == 0
        assert log_file.read_bytes() == b""

    def test_recover_rejects_non_contiguous_offsets(self, tmp_path: Path) -> None:
        log_file = tmp_path / "segment-0000000000.log"
        records = [
            serialize_record(_make_record(offset=0, event_id="evt-0")),
            serialize_record(_make_record(offset=2, event_id="evt-2")),
        ]
        log_file.write_bytes(b"".join(records))

        seg = LogSegment(directory=tmp_path, base_offset=0)
        result = seg.recover()

        assert result.status == "damaged"
        assert result.corruption_offset == len(records[0])


# ── SegmentInfo ──────────────────────────────────────────────────────


class TestSegmentInfo:
    """Tests for SegmentInfo properties."""

    def test_info_matches_segment_state(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path, base_offset=5)
        seg.append(_make_record())
        seg.append(_make_record())

        info = seg.info
        assert isinstance(info, SegmentInfo)
        assert info.base_offset == 5
        assert info.next_offset == 7
        assert info.record_count == 2
        assert info.byte_size > 0
        assert info.is_closed is False

    def test_info_after_close(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path)
        seg.append(_make_record())
        seg.close()

        info = seg.info
        assert info.is_closed is True


# ── close() behaviour ───────────────────────────────────────────────


class TestSegmentClose:
    """Tests for segment close() behaviour."""

    def test_close_saves_index(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path)
        seg.append(_make_record())
        seg.close()

        # Index file should exist
        idx_path = tmp_path / "index-0000000000.idx"
        assert idx_path.exists()

    def test_close_prevents_further_appends(self, tmp_path: Path) -> None:
        seg = _make_segment(tmp_path)
        seg.close()

        with pytest.raises(RuntimeError, match="closed"):
            seg.append(_make_record())

    def test_interval_fsync_is_triggered_when_due(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[int] = []
        monkeypatch.setattr("meshflow.broker.segment.os.fsync", calls.append)
        seg = LogSegment(
            directory=tmp_path,
            base_offset=0,
            fsync_policy="interval",
            fsync_interval_ms=1,
        )
        seg.open()
        seg._last_fsync_ms = 0

        seg.append(_make_record())

        assert len(calls) == 1
