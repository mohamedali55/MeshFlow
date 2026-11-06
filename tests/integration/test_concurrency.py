from __future__ import annotations

import asyncio

import pytest

from meshflow.broker.server import BrokerServer
from meshflow.common.config import BrokerConfig
from meshflow.common.types import Event
from meshflow.consumer import Consumer
from meshflow.producer import Producer


@pytest.mark.asyncio
async def test_concurrent_producers_preserve_all_events(tmp_path) -> None:
    server = BrokerServer(
        BrokerConfig(broker_id="producer-stress", data_dir=tmp_path, port=0),
        topic_partitions={"events": 4},
    )
    await server.start()
    host, port = server.address

    async def publish_batch(producer_id: int) -> list[str]:
        producer = Producer(
            host,
            port,
            sender_id=f"producer-{producer_id}",
            partition_counts={"events": 4},
            max_in_flight=8,
        )
        event_ids: list[str] = []
        for sequence in range(25):
            event = Event.create(
                "events",
                f"key-{producer_id}-{sequence}",
                f"payload-{producer_id}-{sequence}".encode(),
            )
            await producer.publish(event)
            event_ids.append(event.event_id)
        return event_ids

    try:
        batches = await asyncio.gather(*(publish_batch(index) for index in range(8)))
        expected_event_ids = {event_id for batch in batches for event_id in batch}

        consumer = Consumer(host, port, group_id="stress-readers")

        async def consume_partition(partition_id: int) -> list[str]:
            offset = 0
            event_ids: list[str] = []
            while True:
                result = await consumer.fetch(
                    "events",
                    partition_id,
                    offset=offset,
                    max_records=16,
                )
                if not result.records:
                    return event_ids
                event_ids.extend(record.event_id for record in result.records)
                offset = result.records[-1].offset + 1
                await consumer.commit("events", partition_id, offset)

        consumed_batches = await asyncio.gather(
            *(consume_partition(partition_id) for partition_id in range(4))
        )
        consumed_event_ids = [event_id for batch in consumed_batches for event_id in batch]
        assert len(consumed_event_ids) == len(expected_event_ids)
        assert set(consumed_event_ids) == expected_event_ids
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_concurrent_consumers_commit_independent_partitions(tmp_path) -> None:
    server = BrokerServer(
        BrokerConfig(broker_id="consumer-stress", data_dir=tmp_path, port=0),
        topic_partitions={"events": 4},
    )
    await server.start()
    host, port = server.address
    producer = Producer(host, port, partition_counts={"events": 4})

    try:
        events = [
            Event.create("events", f"partition-key-{index}", str(index).encode())
            for index in range(80)
        ]
        await asyncio.gather(*(producer.publish(event) for event in events))

        async def consume_partition(partition_id: int) -> list[str]:
            consumer = Consumer(
                host,
                port,
                group_id="concurrent-readers",
                sender_id=f"reader-{partition_id}",
            )
            result = await consumer.fetch("events", partition_id, max_records=100)
            assert result.records
            await consumer.commit("events", partition_id, result.next_offset)
            restored = await consumer.fetch("events", partition_id)
            assert restored.records == []
            return [record.event_id for record in result.records]

        consumed_event_ids = [
            event_id
            for batch in await asyncio.gather(
                *(consume_partition(partition_id) for partition_id in range(4))
            )
            for event_id in batch
        ]
        assert len(consumed_event_ids) == len(events)
        assert len(set(consumed_event_ids)) == len(events)
    finally:
        await server.close()
