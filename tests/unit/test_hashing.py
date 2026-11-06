"""Unit tests for meshflow.common.hashing."""

from __future__ import annotations

import pytest

from meshflow.common.hashing import select_partition, stable_hash

# ── stable_hash ──────────────────────────────────────────────────────


class TestStableHash:
    """Tests for the stable_hash function."""

    def test_deterministic_same_key(self) -> None:
        """Same key always produces the same hash."""
        assert stable_hash("user-42") == stable_hash("user-42")

    def test_deterministic_across_calls(self) -> None:
        """Multiple calls in a loop remain stable."""
        first = stable_hash("abc")
        for _ in range(100):
            assert stable_hash("abc") == first

    def test_different_keys_produce_different_hashes(self) -> None:
        h1 = stable_hash("key-a")
        h2 = stable_hash("key-b")
        assert h1 != h2

    def test_accepts_bytes(self) -> None:
        """Bytes input works and is equivalent to the UTF-8 encoding of the same string."""
        assert stable_hash(b"hello") == stable_hash("hello")

    def test_returns_unsigned_32bit(self) -> None:
        h = stable_hash("test")
        assert 0 <= h < 2**32

    def test_empty_string(self) -> None:
        h = stable_hash("")
        assert isinstance(h, int)
        assert 0 <= h < 2**32


# ── select_partition ─────────────────────────────────────────────────


class TestSelectPartition:
    """Tests for the select_partition function."""

    def test_result_in_range(self) -> None:
        """Result is always in [0, partition_count)."""
        for n in (1, 2, 3, 7, 16, 128):
            for key in ("a", "b", "c", "user-1", "order-999"):
                p = select_partition(key, n)
                assert 0 <= p < n, f"key={key!r}, n={n}, got p={p}"

    def test_same_key_same_partition(self) -> None:
        """Same key consistently maps to the same partition."""
        p1 = select_partition("user-42", 8)
        p2 = select_partition("user-42", 8)
        assert p1 == p2

    def test_raises_for_zero_partitions(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            select_partition("key", 0)

    def test_raises_for_negative_partitions(self) -> None:
        with pytest.raises(ValueError, match="positive"):
            select_partition("key", -1)

    def test_single_partition_always_zero(self) -> None:
        """With only one partition, every key maps to 0."""
        for key in ("x", "y", "z", "anything"):
            assert select_partition(key, 1) == 0

    def test_distribution_not_all_same(self) -> None:
        """With many distinct keys, at least two different partitions appear."""
        partitions = {select_partition(f"key-{i}", 4) for i in range(50)}
        assert len(partitions) > 1
