"""End-to-end producer, broker, and consumer tests over TCP."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import pytest

from meshflow.broker.server import BrokerServer
from meshflow.common.config import BrokerConfig
from meshflow.common.hashing import select_partition
from meshflow.common.types import Event
from meshflow.consumer import Consumer
from meshflow.producer import Producer
from meshflow.protocol.client import BrokerRequestError
from meshflow.protocol.messages import Envelope, MessageType, read_envelope, write_envelope


@pytest.mark.asyncio
async def test_publish_fetch_commit_and_restart(tmp_path) -> None:
    config = BrokerConfig(
        broker_id="tcp-broker",
        data_dir=tmp_path,
        host="127.0.0.1",
        port=0,
        fsync_policy="every_write",
    )
    server = BrokerServer(config, topic_partitions={"orders": 3})
    await server.start()
    host, port = server.address

    try:
        producer = Producer(host, port, partition_counts={"orders": 3})
        positions = []
        expected_offsets = {partition_id: 0 for partition_id in range(3)}
        for index in range(8):
            position = await producer.publish(
                Event.create("orders", f"customer-{index}", f"order-{index}".encode())
            )
            assert position.offset == expected_offsets[position.partition_id]
            expected_offsets[position.partition_id] += 1
            positions.append(position)
        assert all(
            position.partition_id == select_partition(f"customer-{index}", 3)
            for index, position in enumerate(positions)
        )

        consumer = Consumer(host, port, group_id="order-workers")
        for partition_id in range(3):
            fetched = await consumer.fetch("orders", partition_id)
            assert [record.offset for record in fetched.records] == list(
                range(len(fetched.records))
            )
            if fetched.records:
                await consumer.commit("orders", partition_id, fetched.records[0].offset + 1)

        await server.close()

        restarted = BrokerServer(config, topic_partitions={"orders": 3})
        await restarted.start()
        restarted_host, restarted_port = restarted.address
        restarted_consumer = Consumer(restarted_host, restarted_port, group_id="order-workers")
        try:
            for partition_id in range(3):
                result = await restarted_consumer.fetch("orders", partition_id)
                expected_commit = int(expected_offsets[partition_id] > 0)
                assert result.committed_offset == expected_commit
                if result.committed_offset:
                    assert all(
                        record.offset >= result.committed_offset for record in result.records
                    )
        finally:
            await restarted.close()
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_broker_rejects_wrong_partition_and_invalid_commit(tmp_path) -> None:
    config = BrokerConfig(
        broker_id="validation-broker",
        data_dir=tmp_path,
        host="127.0.0.1",
        port=0,
    )
    server = BrokerServer(config, topic_partitions={"events": 2})
    await server.start()
    host, port = server.address
    event = Event.create("events", "key", b"payload")
    correct_partition = select_partition(event.partition_key, 2)

    try:
        reader, writer = await asyncio.open_connection(host, port)
        request = Envelope.request(
            MessageType.PUBLISH,
            "test-client",
            {
                "topic": "events",
                "partition_id": 1 - correct_partition,
                "event": {
                    "event_id": event.event_id,
                    "partition_key": event.partition_key,
                    "timestamp_ms": event.timestamp_ms,
                    "schema_version": event.schema_version,
                    "headers": event.headers,
                    "payload": "cGF5bG9hZA==",
                },
            },
            timeout=2,
        )
        await write_envelope(writer, request)
        response = await read_envelope(reader)
        writer.close()
        await writer.wait_closed()
        assert response.message_type == MessageType.ERROR
        assert response.payload["code"] == "INVALID_REQUEST"

        consumer = Consumer(host, port, group_id="events-group")
        with pytest.raises(BrokerRequestError, match="exceeds partition end offset"):
            await consumer.commit("events", correct_partition, 1)
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_independent_producer_and_consumer_processes(tmp_path) -> None:
    config = BrokerConfig(
        broker_id="process-broker",
        data_dir=tmp_path,
        host="127.0.0.1",
        port=0,
    )
    server = BrokerServer(config)
    await server.start()
    host, port = server.address
    project_src = Path(__file__).resolve().parents[2] / "src"
    environment = os.environ.copy()
    prior_pythonpath = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = os.pathsep.join(
        path for path in (str(project_src), prior_pythonpath) if path
    )

    async def run_client(code: str) -> str:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise
        assert process.returncode == 0, stderr.decode("utf-8", errors="replace")
        return stdout.decode("utf-8").strip()

    producer_code = f"""
import asyncio, json
from meshflow.common.types import Event
from meshflow.producer import Producer
async def main():
    producer = Producer({host!r}, {port}, sender_id="separate-producer")
    position = await producer.publish(Event.create("jobs", "process-key", b"from-process"))
    print(json.dumps({{"partition_id": position.partition_id, "offset": position.offset}}))
asyncio.run(main())
"""
    try:
        published = json.loads(await run_client(producer_code))
        assert published["offset"] == 0

        consumer_code = f"""
import asyncio, json
from meshflow.consumer import Consumer
async def main():
    consumer = Consumer({host!r}, {port}, group_id="separate-workers")
    result = await consumer.fetch("jobs", {published["partition_id"]})
    assert len(result.records) == 1
    assert result.records[0].payload == b"from-process"
    committed = await consumer.commit("jobs", {published["partition_id"]}, 1)
    print(json.dumps({{"committed": committed}}))
asyncio.run(main())
"""
        committed = json.loads(await run_client(consumer_code))
        assert committed["committed"] == 1
    finally:
        await server.close()
