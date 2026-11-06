"""Unit tests for meshflow.common.types."""

from __future__ import annotations

import time

from meshflow.common.types import (
    Event,
    EventPosition,
    PartitionInfo,
    PartitionStatus,
    TopicConfig,
)

# ── Event.create() ────────────────────────────────────────────────────


class TestEventCreate:
    """Tests for Event.create() factory method."""

    def test_create_generates_unique_ids(self) -> None:
        """Each call to Event.create() produces a distinct event_id."""
        e1 = Event.create("t", "k", b"p1")
        e2 = Event.create("t", "k", b"p2")
        assert e1.event_id != e2.event_id

    def test_create_generates_timestamp(self) -> None:
        """Timestamp is close to current time (within 2 seconds)."""
        before = int(time.time() * 1000)
        event = Event.create("orders", "user-1", b"data")
        after = int(time.time() * 1000)
        assert before <= event.timestamp_ms <= after

    def test_create_preserves_topic_and_key(self) -> None:
        event = Event.create("my-topic", "my-key", b"hello")
        assert event.topic == "my-topic"
        assert event.partition_key == "my-key"
        assert event.payload == b"hello"

    def test_create_default_schema_version(self) -> None:
        event = Event.create("t", "k", b"")
        assert event.schema_version == 1

    def test_create_custom_schema_version(self) -> None:
        event = Event.create("t", "k", b"", schema_version=3)
        assert event.schema_version == 3

    def test_create_default_headers_empty(self) -> None:
        event = Event.create("t", "k", b"")
        assert event.headers == {}

    def test_create_custom_headers(self) -> None:
        hdrs = {"content-type": "application/json"}
        event = Event.create("t", "k", b"", headers=hdrs)
        assert event.headers == hdrs

    def test_event_is_frozen(self) -> None:
        event = Event.create("t", "k", b"")
        try:
            event.topic = "other"  # type: ignore[misc]
            assert False, "Expected FrozenInstanceError"
        except AttributeError:
            pass


# ── PartitionStatus ───────────────────────────────────────────────────


class TestPartitionStatus:
    """Tests for PartitionStatus enum."""

    def test_enum_values(self) -> None:
        assert PartitionStatus.ACTIVE.value == "active"
        assert PartitionStatus.READONLY.value == "readonly"
        assert PartitionStatus.DAMAGED.value == "damaged"
        assert PartitionStatus.RECOVERING.value == "recovering"

    def test_all_members(self) -> None:
        names = {m.name for m in PartitionStatus}
        assert names == {"ACTIVE", "READONLY", "DAMAGED", "RECOVERING"}


# ── EventPosition ────────────────────────────────────────────────────


class TestEventPosition:
    """Tests for EventPosition equality and immutability."""

    def test_equality_same_values(self) -> None:
        a = EventPosition(topic="t", partition_id=0, offset=10)
        b = EventPosition(topic="t", partition_id=0, offset=10)
        assert a == b

    def test_inequality_different_offset(self) -> None:
        a = EventPosition(topic="t", partition_id=0, offset=10)
        b = EventPosition(topic="t", partition_id=0, offset=11)
        assert a != b

    def test_inequality_different_partition(self) -> None:
        a = EventPosition(topic="t", partition_id=0, offset=10)
        b = EventPosition(topic="t", partition_id=1, offset=10)
        assert a != b

    def test_inequality_different_topic(self) -> None:
        a = EventPosition(topic="t1", partition_id=0, offset=10)
        b = EventPosition(topic="t2", partition_id=0, offset=10)
        assert a != b

    def test_hashable(self) -> None:
        """EventPosition can be used as dict key / set member."""
        pos = EventPosition(topic="t", partition_id=0, offset=5)
        assert {pos: 1}[pos] == 1


# ── TopicConfig ──────────────────────────────────────────────────────


class TestTopicConfig:
    """Tests for TopicConfig defaults."""

    def test_defaults(self) -> None:
        tc = TopicConfig(name="orders", partition_count=4)
        assert tc.retention_bytes == 0
        assert tc.retention_ms == 0
        assert tc.replication_factor == 1
        assert tc.partition_algorithm == "md5"
        assert tc.partition_algorithm_version == 1

    def test_custom_values(self) -> None:
        tc = TopicConfig(
            name="events",
            partition_count=8,
            retention_bytes=1024,
            retention_ms=60_000,
            replication_factor=3,
        )
        assert tc.name == "events"
        assert tc.partition_count == 8
        assert tc.retention_bytes == 1024
        assert tc.retention_ms == 60_000
        assert tc.replication_factor == 3


# ── PartitionInfo ────────────────────────────────────────────────────


class TestPartitionInfo:
    """Tests for PartitionInfo defaults."""

    def test_defaults(self) -> None:
        pi = PartitionInfo(topic="t", partition_id=0, leader_broker="b0")
        assert pi.start_offset == 0
        assert pi.next_offset == 0
        assert pi.log_generation == 0
        assert pi.status == PartitionStatus.ACTIVE
