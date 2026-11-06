from __future__ import annotations

import asyncio

import pytest

from meshflow.broker.server import BrokerServer
from meshflow.common.config import BrokerConfig
from meshflow.common.types import Event
from meshflow.consumer import Consumer
from meshflow.coordinator import CoordinatorClient, CoordinatorServer, MetadataStore
from meshflow.producer import Producer
from meshflow.protocol.messages import Envelope, MessageType, read_envelope, write_envelope
from meshflow.worker import Worker


@pytest.mark.asyncio
async def test_publish_is_idempotent_across_retry_and_restart(tmp_path) -> None:
    config = BrokerConfig(broker_id="idempotent", data_dir=tmp_path, port=0)
    event = Event.create("events", "key", b"stable-event")
    server = BrokerServer(config)
    await server.start()
    host, port = server.address
    try:
        producer = Producer(host, port)
        first = await producer.publish(event)
        second = await producer.publish(event)
        assert first == second
        conflicting_retry = Event(
            event_id=event.event_id,
            topic=event.topic,
            partition_key=event.partition_key,
            timestamp_ms=event.timestamp_ms,
            schema_version=event.schema_version,
            payload=b"changed-payload",
            headers=event.headers,
        )
        assert await producer.publish(conflicting_retry) == first
        await server.close()

        index_path = config.partition_dir("events", first.partition_id) / "event-id-index.sqlite3"
        index_path.write_bytes(b"damaged derived index")
        restarted = BrokerServer(config)
        await restarted.start()
        try:
            restarted_host, restarted_port = restarted.address
            result = await Producer(restarted_host, restarted_port).publish(event)
            assert result == first
            fetched = await Consumer(
                restarted_host, restarted_port, group_id="idempotent-readers"
            ).fetch("events", 0)
            assert [record.event_id for record in fetched.records] == [event.event_id]
            assert fetched.records[0].payload == event.payload
        finally:
            await restarted.close()
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_offset_and_application_checkpoint_persist_atomically(tmp_path) -> None:
    config = BrokerConfig(broker_id="checkpoints", data_dir=tmp_path, port=0)
    server = BrokerServer(config)
    await server.start()
    host, port = server.address
    try:
        producer = Producer(host, port)
        for index in range(2):
            await producer.publish(Event.create("events", "key", str(index).encode()))
        consumer = Consumer(host, port, group_id="checkpoint-readers")
        await consumer.fetch("events", 0)
        await consumer.commit("events", 0, 1, checkpoint_state=b"application-state-v1")
        restored = await consumer.fetch("events", 0)
        assert restored.committed_offset == 1
        assert restored.checkpoint_offset == 1
        assert restored.checkpoint_state == b"application-state-v1"
        await server.close()

        restarted = BrokerServer(config)
        await restarted.start()
        try:
            restarted_host, restarted_port = restarted.address
            restored_after_restart = await Consumer(
                restarted_host, restarted_port, group_id="checkpoint-readers"
            ).fetch("events", 0)
            assert restored_after_restart.committed_offset == 1
            assert restored_after_restart.checkpoint_offset == 1
            assert restored_after_restart.checkpoint_state == b"application-state-v1"
        finally:
            await restarted.close()
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_broker_migrates_legacy_offset_metadata(tmp_path) -> None:
    config = BrokerConfig(broker_id="legacy-offsets", data_dir=tmp_path, port=0)
    server = BrokerServer(config)
    await server.start()
    host, port = server.address
    try:
        await Producer(host, port).publish(Event.create("events", "key", b"event"))
    finally:
        await server.close()

    offsets_path = config.metadata_dir / "committed-offsets.json"
    offsets_path.parent.mkdir(parents=True, exist_ok=True)
    offsets_path.write_text('{"legacy-group":{"events":{"0":1}}}', encoding="utf-8")
    restarted = BrokerServer(config)
    await restarted.start()
    try:
        restarted_host, restarted_port = restarted.address
        fetched = await Consumer(restarted_host, restarted_port, group_id="legacy-group").fetch(
            "events", 0
        )
        assert fetched.committed_offset == 1
        assert fetched.records == []
    finally:
        await restarted.close()


@pytest.mark.asyncio
async def test_broker_returns_explicit_overload_response(tmp_path) -> None:
    server = BrokerServer(
        BrokerConfig(broker_id="overloaded", data_dir=tmp_path, port=0),
        max_connections=1,
    )
    await server.start()
    host, port = server.address
    dispatch_started = asyncio.Event()
    release_dispatch = asyncio.Event()
    original_dispatch = server._dispatch

    async def blocked_dispatch(request: Envelope) -> Envelope:
        dispatch_started.set()
        await release_dispatch.wait()
        return server._response(request, MessageType.FETCH_RESPONSE, {})

    server._dispatch = blocked_dispatch

    async def send_request() -> tuple[asyncio.StreamWriter, Envelope]:
        reader, writer = await asyncio.open_connection(host, port)
        await write_envelope(
            writer,
            Envelope.request(
                MessageType.FETCH,
                "test-client",
                {"topic": "events", "partition_id": 0},
                timeout=2,
            ),
        )
        return writer, await read_envelope(reader)

    first_task = asyncio.create_task(send_request())
    await asyncio.wait_for(dispatch_started.wait(), timeout=2)
    try:
        writer, response = await asyncio.wait_for(send_request(), timeout=2)
        assert response.message_type == MessageType.OVERLOADED
        assert response.payload["accepted"] is False
        assert response.payload["retry_after_ms"] > 0
        writer.close()
        await writer.wait_closed()
    finally:
        release_dispatch.set()
        first_writer, _ = await first_task
        first_writer.close()
        await first_writer.wait_closed()
        server._dispatch = original_dispatch
        await server.close()


@pytest.mark.asyncio
async def test_worker_retries_handler_and_publishes_dead_letter_provenance(tmp_path) -> None:
    coordinator = CoordinatorServer(MetadataStore(tmp_path / "coordinator.sqlite"), port=0)
    await coordinator.start()
    coordinator_host, coordinator_port = coordinator.address
    broker = BrokerServer(
        BrokerConfig(broker_id="reliability-broker", data_dir=tmp_path / "broker", port=0),
        coordinator_host=coordinator_host,
        coordinator_port=coordinator_port,
    )
    await broker.start()
    broker_host, broker_port = broker.address
    attempts = 0

    def failing_handler(event: Event) -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError(f"cannot process {event.event_id}")

    worker = Worker(
        "retry-worker",
        "jobs-group",
        ["jobs"],
        coordinator_host=coordinator_host,
        coordinator_port=coordinator_port,
        handler=failing_handler,
        max_handler_retries=2,
        retry_backoff_base=0,
    )
    try:
        await CoordinatorClient(coordinator_host, coordinator_port, sender_id="admin").create_topic(
            "jobs", 1
        )
        event = Event.create("jobs", "key", b"poison")
        await Producer(
            broker_host,
            broker_port,
            coordinator_host=coordinator_host,
            coordinator_port=coordinator_port,
        ).publish(event)
        assert await worker.run_once() == 1
        assert attempts == 3

        reader_coordinator = CoordinatorClient(
            coordinator_host, coordinator_port, sender_id="dlq-reader"
        )
        await reader_coordinator.register_node("dlq-reader", "worker", "127.0.0.1", 0)
        await reader_coordinator.heartbeat("dlq-reader")
        assignments = await reader_coordinator.join_group("dlq-group", "dlq-reader", ["jobs.DLQ"])
        assignment = next(item for item in assignments if item["worker_id"] == "dlq-reader")
        dlq_consumer = Consumer(
            broker_host,
            broker_port,
            group_id="dlq-group",
            sender_id="dlq-reader",
            coordinator_host=coordinator_host,
            coordinator_port=coordinator_port,
        )
        fetched = await dlq_consumer.fetch(
            "jobs.DLQ",
            assignment["partition_id"],
            assignment_epoch=assignment["epoch"],
        )
        assert len(fetched.records) == 1
        dead_letter = fetched.records[0].to_event("jobs.DLQ")
        assert dead_letter.event_id == event.event_id
        assert dead_letter.payload == event.payload
        assert dead_letter.headers["meshflow.original_topic"] == "jobs"
        assert dead_letter.headers["meshflow.original_partition"] == "0"
        assert dead_letter.headers["meshflow.original_offset"] == "0"
        assert dead_letter.headers["meshflow.failure_attempts"] == "3"
        assert "cannot process" in dead_letter.headers["meshflow.failure"]
    finally:
        await broker.close()
        await coordinator.close()
