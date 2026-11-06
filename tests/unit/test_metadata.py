"""Tests for SQLite cluster metadata and partition assignment."""

from __future__ import annotations

import pytest

from meshflow.coordinator.metadata import AssignmentValidationError, MetadataStore


def _register_brokers(store: MetadataStore, count: int) -> None:
    for index in range(count):
        node_id = f"broker-{index}"
        store.register_node(node_id, "broker", "127.0.0.1", 9500 + index)
        store.heartbeat(node_id)


def test_topic_partition_placement_is_distributed_and_persistent(tmp_path) -> None:
    database = tmp_path / "coordinator.sqlite"
    store = MetadataStore(database)
    _register_brokers(store, 3)

    partitions = store.create_topic("events", 7)
    assert [partition["partition_id"] for partition in partitions] == list(range(7))
    assert [partition["broker_id"] for partition in partitions] == [
        "broker-0",
        "broker-1",
        "broker-2",
        "broker-0",
        "broker-1",
        "broker-2",
        "broker-0",
    ]

    reopened = MetadataStore(database)
    assert reopened.get_topic("events") == partitions
    assert reopened.get_partition_route("events", 4)["broker_id"] == "broker-1"


def test_topic_partition_count_cannot_be_changed(tmp_path) -> None:
    store = MetadataStore(tmp_path / "metadata.sqlite")
    _register_brokers(store, 1)
    store.create_topic("events", 3)

    with pytest.raises(ValueError, match="already exists"):
        store.create_topic("events", 4)


def test_greedy_assignments_respect_worker_capacity_and_epochs(tmp_path) -> None:
    store = MetadataStore(tmp_path / "metadata.sqlite")
    _register_brokers(store, 2)
    store.create_topic("events", 8)
    store.register_node("worker-a", "worker", "127.0.0.1", 0, capacity=2)
    store.register_node("worker-b", "worker", "127.0.0.1", 0, capacity=1)
    store.heartbeat("worker-a")
    store.heartbeat("worker-b")

    assignment_a = store.join_group("group-1", "worker-a", ["events"], capacity=2)
    store.join_group("group-1", "worker-b", ["events"], capacity=1)
    all_assignments = store.get_assignments("group-1")

    assert len(assignment_a) == 8
    assert all(item.state == "REVOKING" for item in all_assignments)
    for item in all_assignments:
        store.acknowledge_revocation(
            item.group_id, item.worker_id, item.topic, item.partition_id, item.epoch
        )
    all_assignments = store.get_assignments("group-1")
    assert len(all_assignments) == 8
    assert sum(item.worker_id == "worker-a" for item in all_assignments) == 5
    assert sum(item.worker_id == "worker-b" for item in all_assignments) == 3
    assert all(item.epoch >= 1 for item in all_assignments)

    assert store.get_assignments("group-1", "worker-a") == [
        item for item in all_assignments if item.worker_id == "worker-a"
    ]

    store.register_node("worker-c", "worker", "127.0.0.1", 0, capacity=1)
    store.heartbeat("worker-c")
    store.join_group("group-1", "worker-c", ["events"])
    pending = store.get_assignments("group-1")
    for item in pending:
        store.acknowledge_revocation(
            item.group_id, item.worker_id, item.topic, item.partition_id, item.epoch
        )
    all_assignments = store.get_assignments("group-1")
    assert len(all_assignments) == 8
    assert any(item.worker_id == "worker-c" for item in all_assignments)
    assert all(item.epoch >= 1 for item in all_assignments)
    assert any(item.epoch >= 2 for item in all_assignments)


def test_heartbeat_expiry_rebalances_and_fences_worker(tmp_path) -> None:
    store = MetadataStore(tmp_path / "metadata.sqlite")
    _register_brokers(store, 1)
    store.create_topic("events", 1)
    store.register_node("worker-a", "worker", "127.0.0.1", 0)
    assert (
        next(node for node in store.list_nodes() if node.node_id == "worker-a").status == "STARTING"
    )
    store.heartbeat("worker-a")
    assert (
        next(node for node in store.list_nodes() if node.node_id == "worker-a").status == "HEALTHY"
    )
    store.join_group("group-1", "worker-a", ["events"])

    heartbeat_ms = next(node for node in store.list_nodes() if node.node_id == "worker-a")
    suspected = store.expire_nodes(
        now_ms=heartbeat_ms.last_heartbeat_ms + 1_000,
        heartbeat_timeout_ms=1_000,
        suspect_grace_ms=1_000,
    )
    assert next(node for node in suspected if node.node_id == "worker-a").status == "SUSPECTED"
    assert store.get_assignments("group-1")[0].state == "REVOKING"

    unavailable = store.expire_nodes(
        now_ms=heartbeat_ms.last_heartbeat_ms + 2_000,
        heartbeat_timeout_ms=1_000,
        suspect_grace_ms=1_000,
    )
    assert next(node for node in unavailable if node.node_id == "worker-a").status == "UNAVAILABLE"
    assignment = store.get_assignments("group-1")[0]
    assert assignment.state == "UNASSIGNED"
    assert assignment.epoch == 2
    with pytest.raises(AssignmentValidationError) as fenced:
        store.validate_assignment(
            "group-1",
            "events",
            0,
            "worker-a",
            "broker-0",
            1,
            "COMMIT_OFFSET",
        )
    assert fenced.value.code == "STALE_EPOCH"

    with pytest.raises(ValueError, match="not registered and healthy"):
        store.join_group("group-1", "worker-a", ["events"])


def test_group_rejects_unregistered_worker(tmp_path) -> None:
    store = MetadataStore(tmp_path / "metadata.sqlite")
    _register_brokers(store, 1)
    store.create_topic("events", 1)

    with pytest.raises(ValueError, match="not registered and healthy"):
        store.join_group("group-1", "missing-worker", ["events"])


def test_topic_creation_requires_healthy_broker(tmp_path) -> None:
    store = MetadataStore(tmp_path / "metadata.sqlite")

    with pytest.raises(ValueError, match="healthy broker"):
        store.create_topic("events", 1)
