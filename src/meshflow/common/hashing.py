"""Stable partition hashing for MeshFlow.

Do NOT use Python's built-in hash() for persistent partition assignment.
Python hash randomization (PYTHONHASHSEED) produces different values
across processes and runs.

This module uses MD5 for deterministic, cross-platform partition selection.
MD5 is used purely for distribution, not for cryptographic purposes.
"""

from __future__ import annotations

import hashlib
import struct


def stable_hash(key: str | bytes) -> int:
    """Compute a stable 32-bit hash of the given key.

    The same key always produces the same hash value, regardless of
    Python process, PYTHONHASHSEED, or platform.

    Args:
        key: The partition key to hash. Strings are UTF-8 encoded.

    Returns:
        A 32-bit unsigned integer hash value.
    """
    if isinstance(key, str):
        key = key.encode("utf-8")
    digest = hashlib.md5(key).digest()  # noqa: S324
    return struct.unpack(">I", digest[:4])[0]


def select_partition(key: str | bytes, partition_count: int) -> int:
    """Select a partition for the given key.

    The same key maps to the same partition for a fixed partition count
    and hash algorithm. Changing the partition count can remap keys.

    Args:
        key: The partition key.
        partition_count: Total number of partitions in the topic.

    Returns:
        Partition index in [0, partition_count).

    Raises:
        ValueError: If partition_count is not positive.
    """
    if partition_count <= 0:
        raise ValueError(f"partition_count must be positive, got {partition_count}")
    return stable_hash(key) % partition_count
