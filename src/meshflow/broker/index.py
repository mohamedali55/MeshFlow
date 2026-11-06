"""Sparse index for fast offset lookups within a log segment.

Index binary format (all multi-byte integers are big-endian):

    INDEX_MAGIC:    2 bytes   (0x4D49 = "MI")
    INDEX_VERSION:  1 byte    (0x01)
    ENTRY_COUNT:    4 bytes   (uint32)
    ENTRIES:        16 bytes each
        OFFSET:     8 bytes   (uint64)
        POSITION:   8 bytes   (uint64, byte position in segment file)

The index is an optimization. The log segment remains the source of truth
and the index must be rebuildable from the segment at any time.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from pathlib import Path

INDEX_MAGIC = b"\x4d\x49"  # "MI"
INDEX_VERSION = 1
INDEX_HEADER_SIZE = 7  # MAGIC(2) + VERSION(1) + ENTRY_COUNT(4)
INDEX_ENTRY_SIZE = 16  # OFFSET(8) + POSITION(8)


@dataclass(frozen=True, slots=True)
class IndexEntry:
    """A single entry mapping an offset to a byte position in the segment."""

    offset: int
    position: int


class SparseIndex:
    """In-memory sparse index for a log segment.

    Maps selected offsets to byte positions in the corresponding segment
    file. Not every offset is indexed — entries are added at configurable
    byte intervals. To find a specific offset:

    1. Find the nearest preceding index entry.
    2. Scan forward from that byte position in the segment.
    """

    def __init__(self, path: Path, interval_bytes: int = 4096) -> None:
        """Initialize a sparse index.

        Args:
            path: File path for the index file (.idx).
            interval_bytes: Minimum bytes between index entries.
        """
        self._path = path
        self._interval_bytes = interval_bytes
        self._entries: list[IndexEntry] = []
        self._bytes_since_last_entry = 0

    @property
    def path(self) -> Path:
        """Path to the index file."""
        return self._path

    @property
    def entries(self) -> list[IndexEntry]:
        """All index entries in offset order."""
        return list(self._entries)

    @property
    def entry_count(self) -> int:
        """Number of entries in the index."""
        return len(self._entries)

    def maybe_add_entry(self, offset: int, position: int, record_bytes: int) -> bool:
        """Conditionally add an index entry based on the byte interval.

        An entry is added if enough bytes have accumulated since the last
        entry (or if this is the first entry).

        Args:
            offset: The record's offset.
            position: The record's byte position in the segment file.
            record_bytes: Size of the record in bytes.

        Returns:
            True if an entry was added, False otherwise.
        """
        if not self._entries or self._bytes_since_last_entry >= self._interval_bytes:
            self._entries.append(IndexEntry(offset=offset, position=position))
            self._bytes_since_last_entry = record_bytes
            return True
        self._bytes_since_last_entry += record_bytes
        return False

    def force_add_entry(self, offset: int, position: int) -> None:
        """Unconditionally add an index entry. Used during index rebuilding."""
        self._entries.append(IndexEntry(offset=offset, position=position))
        self._bytes_since_last_entry = 0

    def lookup(self, target_offset: int) -> IndexEntry | None:
        """Find the nearest index entry at or before the target offset.

        Uses binary search for O(log n) lookup.

        Args:
            target_offset: The offset to look up.

        Returns:
            The nearest preceding IndexEntry, or None if no entry exists
            at or before the target offset.
        """
        if not self._entries:
            return None

        lo, hi = 0, len(self._entries) - 1
        result = None

        while lo <= hi:
            mid = (lo + hi) // 2
            if self._entries[mid].offset <= target_offset:
                result = self._entries[mid]
                lo = mid + 1
            else:
                hi = mid - 1

        return result

    def save(self) -> None:
        """Write the index to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = INDEX_MAGIC + struct.pack(">B", INDEX_VERSION)
        data += struct.pack(">I", len(self._entries))
        for entry in self._entries:
            data += struct.pack(">QQ", entry.offset, entry.position)

        with open(self._path, "wb") as f:
            f.write(data)
            f.flush()

    def load(self) -> bool:
        """Load the index from disk.

        Returns:
            True if the index was loaded successfully, False if the file
            doesn't exist or is corrupt (the index can be rebuilt).
        """
        if not self._path.exists():
            return False

        try:
            with open(self._path, "rb") as f:
                header = f.read(INDEX_HEADER_SIZE)
                if len(header) < INDEX_HEADER_SIZE:
                    return False

                magic = header[0:2]
                if magic != INDEX_MAGIC:
                    return False

                version = header[2]
                if version != INDEX_VERSION:
                    return False

                (entry_count,) = struct.unpack(">I", header[3:7])
                expected_size = INDEX_HEADER_SIZE + entry_count * INDEX_ENTRY_SIZE
                if self._path.stat().st_size != expected_size:
                    return False

                remaining = f.read(entry_count * INDEX_ENTRY_SIZE)
                if len(remaining) != entry_count * INDEX_ENTRY_SIZE:
                    return False

                entries = []
                for i in range(entry_count):
                    offset_pos = i * INDEX_ENTRY_SIZE
                    offset, position = struct.unpack(
                        ">QQ", remaining[offset_pos : offset_pos + INDEX_ENTRY_SIZE]
                    )
                    entries.append(IndexEntry(offset=offset, position=position))

                if any(
                    current.offset <= previous.offset or current.position <= previous.position
                    for previous, current in zip(entries, entries[1:])
                ):
                    return False

                self._entries = entries
                self._bytes_since_last_entry = 0
                return True

        except OSError:
            return False

    def clear(self) -> None:
        """Clear all entries from the in-memory index."""
        self._entries.clear()
        self._bytes_since_last_entry = 0

    def delete(self) -> None:
        """Delete the index file from disk and clear in-memory entries."""
        self.clear()
        if self._path.exists():
            self._path.unlink()
