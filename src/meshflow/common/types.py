"""Core data types for MeshFlow.

Defines the fundamental data structures used across the system:
Event, Topic, Partition, and related enums.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class PartitionStatus(Enum):
    """Status of a partition within a broker."""

    ACTIVE = "active"
    READONLY = "readonly"
    DAMAGED = "damaged"
    RECOVERING = "recovering"


@dataclass(frozen=True, slots=True)
class Event:
    """An event to be published to a topic.

    The broker's event envelope is separate from application-specific
    payload schemas. The payload is opaque bytes from the broker's
    perspective.
    """

    event_id: str
    topic: str
    partition_key: str
    timestamp_ms: int
    schema_version: int
    payload: bytes
    headers: dict[str, str] = field(default_factory=dict)

    @staticmethod
    def create(
        topic: str,
        partition_key: str,
        payload: bytes,
        *,
        headers: dict[str, str] | None = None,
        schema_version: int = 1,
    ) -> Event:
        """Create a new event with an auto-generated ID and timestamp."""
        return Event(
            event_id=str(uuid.uuid4()),
            topic=topic,
            partition_key=partition_key,
            timestamp_ms=int(time.time() * 1000),
            schema_version=schema_version,
            payload=payload,
            headers=headers or {},
        )


@dataclass(slots=True)
class TopicConfig:
    """Configuration for a topic.

    Attributes:
        name: Topic name.
        partition_count: Number of partitions. Do not change silently in v0.1.
        retention_bytes: Maximum bytes to retain per partition (0 = unlimited).
        retention_ms: Maximum age in milliseconds (0 = unlimited).
        replication_factor: Number of synchronous leader/follower replicas.
        partition_algorithm: Hash algorithm used for partition selection.
        partition_algorithm_version: Version of the algorithm for migration safety.
    """

    name: str
    partition_count: int
    retention_bytes: int = 0
    retention_ms: int = 0
    replication_factor: int = 1
    partition_algorithm: str = "md5"
    partition_algorithm_version: int = 1


@dataclass(slots=True)
class PartitionInfo:
    """Metadata about a single partition.

    Attributes:
        topic: Owning topic name.
        partition_id: Partition index within the topic.
        leader_broker: ID of the broker hosting this partition.
        start_offset: First valid offset (may advance after retention cleanup).
        next_offset: Next offset to be assigned to a new record.
        log_generation: Incremented on recovery or ownership change.
        status: Current partition status.
    """

    topic: str
    partition_id: int
    leader_broker: str
    start_offset: int = 0
    next_offset: int = 0
    log_generation: int = 0
    status: PartitionStatus = PartitionStatus.ACTIVE


@dataclass(frozen=True, slots=True)
class EventPosition:
    """Identifies an event's position in the log.

    Offsets are partition-local. Offset 15 in one partition is unrelated
    to offset 15 in another partition.
    """

    topic: str
    partition_id: int
    offset: int
