"""Unit tests for meshflow.broker.index."""

from __future__ import annotations

import struct
from pathlib import Path

from meshflow.broker.index import (
    INDEX_MAGIC,
    INDEX_VERSION,
    IndexEntry,
    SparseIndex,
)

# ── Helpers ──────────────────────────────────────────────────────────


def _make_index(tmp_path: Path, interval_bytes: int = 100) -> SparseIndex:
    return SparseIndex(tmp_path / "test.idx", interval_bytes=interval_bytes)


# ── maybe_add_entry and lookup ───────────────────────────────────────


class TestMaybeAddEntryAndLookup:
    """Tests for adding entries and looking them up."""

    def test_add_first_entry_always_added(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path)
        assert idx.maybe_add_entry(offset=0, position=0, record_bytes=50)
        assert idx.entry_count == 1

    def test_lookup_exact_offset(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path)
        idx.maybe_add_entry(offset=0, position=0, record_bytes=50)
        idx.force_add_entry(offset=10, position=500)

        entry = idx.lookup(10)
        assert entry is not None
        assert entry.offset == 10
        assert entry.position == 500

    def test_lookup_returns_nearest_preceding_entry(self, tmp_path: Path) -> None:
        """Binary search returns the closest entry at or before the target."""
        idx = _make_index(tmp_path, interval_bytes=0)
        idx.force_add_entry(offset=0, position=0)
        idx.force_add_entry(offset=10, position=500)
        idx.force_add_entry(offset=20, position=1000)
        idx.force_add_entry(offset=30, position=1500)

        entry = idx.lookup(15)
        assert entry is not None
        assert entry.offset == 10
        assert entry.position == 500

    def test_lookup_returns_none_when_no_entry_before_target(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path)
        # Only entry is at offset 10
        idx.force_add_entry(offset=10, position=500)

        result = idx.lookup(5)
        assert result is None

    def test_lookup_empty_index_returns_none(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path)
        assert idx.lookup(0) is None

    def test_lookup_last_entry(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path, interval_bytes=0)
        idx.force_add_entry(offset=0, position=0)
        idx.force_add_entry(offset=5, position=100)
        idx.force_add_entry(offset=10, position=200)

        entry = idx.lookup(100)  # well beyond all entries
        assert entry is not None
        assert entry.offset == 10


# ── Save and load round-trip ─────────────────────────────────────────


class TestSaveAndLoad:
    """Tests for persisting and loading the index."""

    def test_save_and_load_round_trip(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path, interval_bytes=0)
        idx.force_add_entry(offset=0, position=0)
        idx.force_add_entry(offset=5, position=200)
        idx.force_add_entry(offset=10, position=500)
        idx.save()

        idx2 = _make_index(tmp_path, interval_bytes=0)
        assert idx2.load() is True
        assert idx2.entry_count == 3

        entries = idx2.entries
        assert entries[0] == IndexEntry(offset=0, position=0)
        assert entries[1] == IndexEntry(offset=5, position=200)
        assert entries[2] == IndexEntry(offset=10, position=500)

    def test_load_returns_false_for_missing_file(self, tmp_path: Path) -> None:
        idx = SparseIndex(tmp_path / "nonexistent.idx")
        assert idx.load() is False

    def test_load_returns_false_for_corrupt_file(self, tmp_path: Path) -> None:
        idx_path = tmp_path / "corrupt.idx"
        idx_path.write_bytes(b"\x00\x01\x02")  # garbage data

        idx = SparseIndex(idx_path)
        assert idx.load() is False

    def test_load_returns_false_for_truncated_file(self, tmp_path: Path) -> None:
        """Write valid header but truncate entries."""
        idx_path = tmp_path / "truncated.idx"
        # Write valid header claiming 5 entries, but provide none
        header = INDEX_MAGIC + struct.pack(">B", INDEX_VERSION) + struct.pack(">I", 5)
        idx_path.write_bytes(header)

        idx = SparseIndex(idx_path)
        assert idx.load() is False

    def test_load_returns_false_for_trailing_bytes(self, tmp_path: Path) -> None:
        idx_path = tmp_path / "trailing.idx"
        idx = SparseIndex(idx_path)
        idx.force_add_entry(offset=0, position=0)
        idx.save()
        idx_path.write_bytes(idx_path.read_bytes() + b"\x00")

        assert SparseIndex(idx_path).load() is False

    def test_load_returns_false_for_non_increasing_entries(self, tmp_path: Path) -> None:
        idx_path = tmp_path / "unordered.idx"
        header = INDEX_MAGIC + struct.pack(">B", INDEX_VERSION) + struct.pack(">I", 2)
        entries = struct.pack(">QQ", 10, 100) + struct.pack(">QQ", 9, 200)
        idx_path.write_bytes(header + entries)

        assert SparseIndex(idx_path).load() is False

    def test_load_bounds_claimed_entry_count_by_file_size(self, tmp_path: Path) -> None:
        idx_path = tmp_path / "oversized-count.idx"
        header = INDEX_MAGIC + struct.pack(">B", INDEX_VERSION) + struct.pack(">I", 0xFFFFFFFF)
        idx_path.write_bytes(header)

        assert SparseIndex(idx_path).load() is False


# ── clear() and delete() ────────────────────────────────────────────


class TestClearAndDelete:
    """Tests for clear() and delete() operations."""

    def test_clear_resets_entries(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path, interval_bytes=0)
        idx.force_add_entry(offset=0, position=0)
        idx.force_add_entry(offset=1, position=100)
        assert idx.entry_count == 2

        idx.clear()
        assert idx.entry_count == 0
        assert idx.entries == []

    def test_delete_removes_file_and_clears(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path, interval_bytes=0)
        idx.force_add_entry(offset=0, position=0)
        idx.save()
        assert idx.path.exists()

        idx.delete()
        assert not idx.path.exists()
        assert idx.entry_count == 0


# ── maybe_add_entry respects interval_bytes ──────────────────────────


class TestIntervalBytes:
    """Tests that maybe_add_entry respects the byte interval threshold."""

    def test_second_entry_not_added_before_threshold(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path, interval_bytes=100)
        # First entry always added
        assert idx.maybe_add_entry(offset=0, position=0, record_bytes=50)
        # Second entry: only 50 bytes accumulated, threshold is 100
        assert not idx.maybe_add_entry(offset=1, position=50, record_bytes=40)
        assert idx.entry_count == 1

    def test_entry_added_after_threshold_reached(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path, interval_bytes=100)
        assert idx.maybe_add_entry(offset=0, position=0, record_bytes=50)
        # Accumulate: 50 + 60 = 110 >= 100
        idx.maybe_add_entry(offset=1, position=50, record_bytes=60)
        assert idx.maybe_add_entry(offset=2, position=110, record_bytes=30)
        assert idx.entry_count == 2  # entry at offset 0 and offset 2

    def test_force_add_always_adds(self, tmp_path: Path) -> None:
        idx = _make_index(tmp_path, interval_bytes=10000)
        idx.force_add_entry(offset=0, position=0)
        idx.force_add_entry(offset=1, position=50)
        idx.force_add_entry(offset=2, position=100)
        assert idx.entry_count == 3
