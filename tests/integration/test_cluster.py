"""Coordinator and multi-broker integration tests."""

from __future__ import annotations

import asyncio

import pytest

from meshflow.broker.server import BrokerServer
from meshflow.common.config import BrokerConfig
from meshflow.common.types import Event
from meshflow.consumer import Consumer
from meshflow.coordinator import CoordinatorClient, CoordinatorServer, MetadataStore
from meshflow.coordinator.optimizer import PressureConfig
from meshflow.producer import Producer
from meshflow.protocol.client import BrokerRequestError
from meshflow.worker import Worker


@pytest.mark.asyncio
async def test_coordinator_routes_events_across_multiple_brokers(tmp_path) -> None:
    coordinator = CoordinatorServer(
        MetadataStore(tmp_path / "coordinator.sqlite"),
        port=0,
    )
    await coordinator.start()
    coordinator_host, coordinator_port = coordinator.address
    brokers = [
        BrokerServer(
            BrokerConfig(
                broker_id=f"broker-{index}",
                data_dir=tmp_path / f"broker-data-{index}",
                host="127.0.0.1",
                port=0,
            ),
            coordinator_host=coordinator_host,
            coordinator_port=coordinator_port,
        )
        for index in range(2)
    ]
    try:
        for broker in brokers:
            await broker.start()
        client = CoordinatorClient(coordinator_host, coordinator_port, sender_id="admin-client")
        routes = await client.create_topic("events", 6)
        assert {route["broker_id"] for route in routes} == {"broker-0", "broker-1"}

        first_broker_host, first_broker_port = brokers[0].address
        producer = Producer(
            first_broker_host,
            first_broker_port,
            coordinator_host=coordinator_host,
            coordinator_port=coordinator_port,
        )
        positions = [
            await producer.publish(
                Event.create("events", f"key-{index}", f"payload-{index}".encode())
            )
            for index in range(18)
        ]
        assert {position.partition_id for position in positions} == set(range(6))

        consumer = Consumer(
            first_broker_host,
            first_broker_port,
            group_id="event-processors",
            sender_id="event-reader",
            coordinator_host=coordinator_host,
            coordinator_port=coordinator_port,
        )
        reader_client = CoordinatorClient(
            coordinator_host, coordinator_port, sender_id="event-reader"
        )
        await reader_client.register_node("event-reader", "worker", "127.0.0.1", 0)
        await reader_client.heartbeat("event-reader")
        assignments = await reader_client.join_group("event-processors", "event-reader", ["events"])
        epochs = {item["partition_id"]: item["epoch"] for item in assignments}
        fetched_total = 0
        for partition_id in range(6):
            fetched = await consumer.fetch(
                "events", partition_id, assignment_epoch=epochs[partition_id]
            )
            fetched_total += len(fetched.records)
            assert all(record.offset == index for index, record in enumerate(fetched.records))
            if fetched.records:
                await consumer.commit(
                    "events",
                    partition_id,
                    len(fetched.records),
                    assignment_epoch=epochs[partition_id],
                )
        assert fetched_total == 18
    finally:
        for broker in brokers:
            await broker.close()
        await coordinator.close()


@pytest.mark.asyncio
async def test_workers_get_distinct_assignments_and_process_events(tmp_path) -> None:
    coordinator = CoordinatorServer(
        MetadataStore(tmp_path / "coordinator.sqlite"),
        port=0,
    )
    await coordinator.start()
    coordinator_host, coordinator_port = coordinator.address
    broker = BrokerServer(
        BrokerConfig(
            broker_id="worker-broker",
            data_dir=tmp_path / "broker-data",
            host="127.0.0.1",
            port=0,
        ),
        coordinator_host=coordinator_host,
        coordinator_port=coordinator_port,
    )
    await broker.start()
    broker_host, broker_port = broker.address
    try:
        admin = CoordinatorClient(coordinator_host, coordinator_port, sender_id="cluster-admin")
        await admin.create_topic("jobs", 4)
        producer = Producer(
            broker_host,
            broker_port,
            coordinator_host=coordinator_host,
            coordinator_port=coordinator_port,
        )
        processed: dict[str, list[str]] = {"worker-a": [], "worker-b": []}

        def handler(worker_id: str):
            def process(event: Event) -> None:
                processed[worker_id].append(event.event_id)

            return process

        workers = [
            Worker(
                worker_id,
                "job-workers",
                ["jobs"],
                coordinator_host=coordinator_host,
                coordinator_port=coordinator_port,
                handler=handler(worker_id),
            )
            for worker_id in ("worker-a", "worker-b")
        ]
        await asyncio.gather(*(worker.register() for worker in workers))
        for worker in workers:
            assert await worker.run_once() == 0
        assert await workers[0].run_once() == 0
        assert sum(await asyncio.gather(*(worker.run_once() for worker in workers))) == 0

        for index in range(12):
            await producer.publish(
                Event.create("jobs", f"job-key-{index}", f"job-{index}".encode())
            )

        results = await asyncio.gather(*(worker.run_once() for worker in workers))
        assert sum(results) == 12
        assert len(processed["worker-a"]) > 0
        assert len(processed["worker-b"]) > 0
        assert len(set(processed["worker-a"]) & set(processed["worker-b"])) == 0
    finally:
        await broker.close()
        await coordinator.close()


@pytest.mark.asyncio
async def test_rebalance_drains_inflight_work_and_fences_old_epochs(tmp_path) -> None:
    coordinator = CoordinatorServer(
        MetadataStore(tmp_path / "coordinator.sqlite"),
        port=0,
    )
    await coordinator.start()
    coordinator_host, coordinator_port = coordinator.address
    broker = BrokerServer(
        BrokerConfig(
            broker_id="fence-broker",
            data_dir=tmp_path / "broker-data",
            host="127.0.0.1",
            port=0,
        ),
        coordinator_host=coordinator_host,
        coordinator_port=coordinator_port,
    )
    await broker.start()
    broker_host, broker_port = broker.address
    try:
        admin = CoordinatorClient(coordinator_host, coordinator_port, sender_id="fence-admin")
        await admin.create_topic("fenced-events", 2)
        first = CoordinatorClient(coordinator_host, coordinator_port, sender_id="worker-a")
        await first.register_node("worker-a", "worker", "127.0.0.1", 0)
        await first.heartbeat("worker-a")
        initial = await first.join_group("fenced-group", "worker-a", ["fenced-events"])
        old_epoch = next(item["epoch"] for item in initial if item["partition_id"] == 1)

        producer = Producer(
            broker_host,
            broker_port,
            coordinator_host=coordinator_host,
            coordinator_port=coordinator_port,
        )
        published = await producer.publish(
            Event.create("fenced-events", "fenced-key-0", b"payload")
        )
        key_index = 0
        while published.partition_id != 1:
            key_index += 1
            published = await producer.publish(
                Event.create("fenced-events", f"fenced-key-{key_index}", b"payload")
            )
        consumer = Consumer(
            broker_host,
            broker_port,
            group_id="fenced-group",
            sender_id="worker-a",
        )
        with pytest.raises(BrokerRequestError) as missing_epoch:
            await consumer.fetch("fenced-events", 1)
        assert missing_epoch.value.code == "INVALID_REQUEST"
        fetched = await consumer.fetch("fenced-events", 1, assignment_epoch=old_epoch)
        assert len(fetched.records) >= 1

        second = CoordinatorClient(coordinator_host, coordinator_port, sender_id="worker-b")
        await second.register_node("worker-b", "worker", "127.0.0.1", 0)
        await second.heartbeat("worker-b")
        assert await second.join_group("fenced-group", "worker-b", ["fenced-events"]) == []

        with pytest.raises(BrokerRequestError) as fetch_error:
            await consumer.fetch("fenced-events", 1, assignment_epoch=old_epoch)
        assert fetch_error.value.code == "ASSIGNMENT_REVOKING"

        assert (
            await consumer.commit(
                "fenced-events", 1, fetched.next_offset, assignment_epoch=old_epoch
            )
            == fetched.next_offset
        )
        await first.acknowledge_revocation(
            "fenced-group", "worker-a", "fenced-events", 1, old_epoch
        )
        for assignment in await admin.get_assignments("fenced-group"):
            if assignment["state"] == "REVOKING":
                await first.acknowledge_revocation(
                    "fenced-group",
                    "worker-a",
                    assignment["topic"],
                    assignment["partition_id"],
                    assignment["epoch"],
                )
        current = await admin.get_assignments("fenced-group")
        moved = next(item for item in current if item["partition_id"] == 1)
        assert moved["worker_id"] == "worker-b"
        assert moved["epoch"] == old_epoch + 1

        with pytest.raises(BrokerRequestError) as commit_error:
            await consumer.commit(
                "fenced-events", 1, fetched.next_offset, assignment_epoch=old_epoch
            )
        assert commit_error.value.code == "STALE_EPOCH"
    finally:
        await broker.close()
        await coordinator.close()


@pytest.mark.asyncio
async def test_worker_stops_with_a_drained_group_leave(tmp_path) -> None:
    coordinator = CoordinatorServer(
        MetadataStore(tmp_path / "coordinator.sqlite"),
        port=0,
    )
    await coordinator.start()
    coordinator_host, coordinator_port = coordinator.address
    broker = BrokerServer(
        BrokerConfig(
            broker_id="leave-broker",
            data_dir=tmp_path / "broker-data",
            host="127.0.0.1",
            port=0,
        ),
        coordinator_host=coordinator_host,
        coordinator_port=coordinator_port,
    )
    await broker.start()
    try:
        admin = CoordinatorClient(coordinator_host, coordinator_port, sender_id="leave-admin")
        await admin.create_topic("leave-events", 1)
        worker = Worker(
            "leaving-worker",
            "leave-group",
            ["leave-events"],
            coordinator_host=coordinator_host,
            coordinator_port=coordinator_port,
            poll_interval=0.01,
            heartbeat_interval=0.01,
        )
        task = asyncio.create_task(worker.run())
        try:
            async with asyncio.timeout(2):
                assignments = await admin.get_assignments("leave-group")
                while not assignments:
                    await asyncio.sleep(0.01)
                    assignments = await admin.get_assignments("leave-group")
            assert assignments[0]["state"] == "ACTIVE"
            worker.stop()
            await asyncio.wait_for(task, timeout=2)
            assignments = await admin.get_assignments("leave-group")
            assert assignments[0]["state"] == "UNASSIGNED"
            assert assignments[0]["epoch"] == 2
        finally:
            if not task.done():
                worker.stop()
                await asyncio.wait_for(task, timeout=2)
    finally:
        await broker.close()
        await coordinator.close()


@pytest.mark.asyncio
async def test_optimizer_moves_high_lag_partition_and_verifies_outcome(tmp_path) -> None:
    config = PressureConfig(
        alpha=1.0,
        overloaded_enter=0.8,
        overloaded_exit=0.5,
        minimum_benefit=0.1,
        migration_cost=0.05,
        cooldown_seconds=0,
    )
    coordinator = CoordinatorServer(
        MetadataStore(tmp_path / "coordinator.sqlite"),
        port=0,
        optimizer_config=config,
    )
    await coordinator.start()
    coordinator_host, coordinator_port = coordinator.address
    broker = BrokerServer(
        BrokerConfig(
            broker_id="optimizer-broker",
            data_dir=tmp_path / "broker-data",
            host="127.0.0.1",
            port=0,
        ),
        coordinator_host=coordinator_host,
        coordinator_port=coordinator_port,
    )
    await broker.start()
    try:
        admin = CoordinatorClient(coordinator_host, coordinator_port, sender_id="optimizer-admin")
        await admin.create_topic("optimizer-events", 2)
        workers = {
            worker_id: CoordinatorClient(coordinator_host, coordinator_port, sender_id=worker_id)
            for worker_id in ("optimizer-worker-a", "optimizer-worker-b")
        }
        for worker_id, client in workers.items():
            await client.register_node(worker_id, "worker", "127.0.0.1", 0)
            await client.heartbeat(worker_id)
        await workers["optimizer-worker-a"].join_group(
            "optimizer-group", "optimizer-worker-a", ["optimizer-events"]
        )
        await workers["optimizer-worker-b"].join_group(
            "optimizer-group", "optimizer-worker-b", ["optimizer-events"]
        )
        revoking = await admin.get_assignments("optimizer-group")
        for assignment in revoking:
            await workers[assignment["worker_id"]].acknowledge_revocation(
                "optimizer-group",
                assignment["worker_id"],
                assignment["topic"],
                assignment["partition_id"],
                assignment["epoch"],
            )
        assignments = await admin.get_assignments("optimizer-group")
        assert all(item["state"] == "ACTIVE" for item in assignments)
        source_worker = assignments[0]["worker_id"]
        target_worker = next(worker_id for worker_id in workers if worker_id != source_worker)
        source_assignments = [item for item in assignments if item["worker_id"] == source_worker]
        target_assignments = [item for item in assignments if item["worker_id"] == target_worker]
        assert source_assignments and target_assignments

        def metric_report(
            worker_id: str,
            owned: list[dict[str, object]],
            *,
            cpu: float,
            memory: float,
            queue: int,
            latency: float,
            lags: dict[int, int],
        ) -> dict[str, object]:
            return {
                "group_id": "optimizer-group",
                "cpu_percent": cpu,
                "memory_percent": memory,
                "queue_depth": queue,
                "queue_capacity": 100,
                "p95_latency_ms": latency,
                "processing_rate": 10.0,
                "in_flight": 0,
                "partition_lags": [
                    {
                        "group_id": "optimizer-group",
                        "topic": assignment["topic"],
                        "partition_id": assignment["partition_id"],
                        "lag_records": lags.get(assignment["partition_id"], 0),
                    }
                    for assignment in owned
                ],
            }

        target_report = metric_report(
            target_worker,
            target_assignments,
            cpu=10,
            memory=10,
            queue=5,
            latency=50,
            lags={},
        )
        await workers[target_worker].report_metrics("target-instance", 1000, target_report)
        high_source_report = metric_report(
            source_worker,
            source_assignments,
            cpu=95,
            memory=90,
            queue=95,
            latency=900,
            lags={source_assignments[0]["partition_id"]: 100},
        )
        report = await workers[source_worker].report_metrics(
            "source-instance", 1000, high_source_report
        )
        current_metrics = await admin.metrics()
        assert {item["node_id"] for item in current_metrics} == set(workers)
        assert all(0 <= item["ema_pressure"] <= 1 for item in current_metrics)
        decision = report["decision"]
        assert decision["decision_type"] == "MOVE_PARTITION"
        assert decision["source_worker"] == source_worker
        assert decision["destination_worker"] == target_worker
        assert decision["partition_id"] == source_assignments[0]["partition_id"]
        assert decision["predicted_benefit"] > 0
        blocked = await admin.optimize_group("optimizer-group")
        assert blocked["decision_type"] == "NO_ACTION"
        assert "awaiting revocation" in blocked["reason"]

        moved_partition = decision["partition_id"]
        revoke = await admin.get_assignments("optimizer-group")
        moving = next(item for item in revoke if item["partition_id"] == moved_partition)
        assert moving["state"] == "REVOKING"
        old_epoch = moving["epoch"]
        await workers[source_worker].acknowledge_revocation(
            "optimizer-group",
            source_worker,
            moving["topic"],
            moved_partition,
            old_epoch,
        )
        assignments = await admin.get_assignments("optimizer-group")
        moved = next(item for item in assignments if item["partition_id"] == moved_partition)
        assert moved["worker_id"] == target_worker
        assert moved["epoch"] == old_epoch + 1

        new_source_assignments = [
            item for item in assignments if item["worker_id"] == source_worker
        ]
        new_target_assignments = [
            item for item in assignments if item["worker_id"] == target_worker
        ]
        await workers[source_worker].report_metrics(
            "source-instance",
            1000,
            metric_report(
                source_worker,
                new_source_assignments,
                cpu=20,
                memory=20,
                queue=10,
                latency=100,
                lags={},
            ),
        )
        await workers[target_worker].report_metrics(
            "target-instance",
            1000,
            metric_report(
                target_worker,
                new_target_assignments,
                cpu=15,
                memory=15,
                queue=10,
                latency=100,
                lags={},
            ),
        )
        decisions = await admin.decisions("optimizer-group")
        completed = next(
            item for item in decisions if item["decision_id"] == decision["decision_id"]
        )
        assert completed["status"] == "VERIFIED"
        assert completed["actual_benefit"] is not None
        assert completed["prediction_error"] is not None
    finally:
        await broker.close()
        await coordinator.close()


@pytest.mark.asyncio
async def test_three_brokers_and_four_independent_worker_processes(tmp_path) -> None:
    import os
    import sys
    from pathlib import Path

    coordinator = CoordinatorServer(
        MetadataStore(tmp_path / "coordinator.sqlite"),
        port=0,
    )
    await coordinator.start()
    coordinator_host, coordinator_port = coordinator.address
    brokers = [
        BrokerServer(
            BrokerConfig(
                broker_id=f"cluster-broker-{index}",
                data_dir=tmp_path / f"cluster-broker-data-{index}",
                host="127.0.0.1",
                port=0,
            ),
            coordinator_host=coordinator_host,
            coordinator_port=coordinator_port,
        )
        for index in range(3)
    ]
    try:
        for broker in brokers:
            await broker.start()
        admin = CoordinatorClient(coordinator_host, coordinator_port, sender_id="topology-admin")
        routes = await admin.create_topic("cluster-events", 12)
        assert {route["broker_id"] for route in routes} == {
            "cluster-broker-0",
            "cluster-broker-1",
            "cluster-broker-2",
        }

        source_path = Path(__file__).resolve().parents[2] / "src"
        environment = os.environ.copy()
        environment["PYTHONPATH"] = os.pathsep.join(
            item for item in (str(source_path), environment.get("PYTHONPATH", "")) if item
        )
        for index in range(4):
            worker_id = f"cluster-worker-{index}"
            code = f"""
import asyncio
from meshflow.coordinator import CoordinatorClient
async def main():
    client = CoordinatorClient({coordinator_host!r}, {coordinator_port},
                               sender_id={worker_id!r}, max_retries=0)
    await client.register_node({worker_id!r}, "worker", "127.0.0.1", 0)
    await client.heartbeat({worker_id!r})
    await client.join_group("cluster-group", {worker_id!r}, ["cluster-events"])
asyncio.run(main())
"""
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                code,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=environment,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
            assert process.returncode == 0, stdout.decode(
                "utf-8", errors="replace"
            ) + stderr.decode("utf-8", errors="replace")

        worker_id = "cluster-worker-0"
        finalize_code = f"""
import asyncio
from meshflow.coordinator import CoordinatorClient
async def main():
    client = CoordinatorClient({coordinator_host!r}, {coordinator_port},
                               sender_id={worker_id!r}, max_retries=0)
    await client.heartbeat({worker_id!r})
    assignments = await client.join_group(
        "cluster-group", {worker_id!r}, ["cluster-events"]
    )
    for assignment in assignments:
        if assignment["state"] == "REVOKING":
            await client.acknowledge_revocation(
                "cluster-group", {worker_id!r}, assignment["topic"],
                assignment["partition_id"], assignment["epoch"]
            )
asyncio.run(main())
"""
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            finalize_code,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=environment,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
        assert process.returncode == 0, stdout.decode("utf-8", errors="replace") + stderr.decode(
            "utf-8", errors="replace"
        )

        assignments = await admin.get_assignments("cluster-group")
        assert len(assignments) == 12
        counts = {
            worker_id: sum(item["worker_id"] == worker_id for item in assignments)
            for worker_id in (f"cluster-worker-{index}" for index in range(4))
        }
        assert counts == {f"cluster-worker-{index}": 3 for index in range(4)}
        assert all(item["epoch"] >= 1 for item in assignments)

        producer = Producer(
            "127.0.0.1",
            9500,
            coordinator_host=coordinator_host,
            coordinator_port=coordinator_port,
        )
        expected_offsets = [0] * 12
        for index in range(36):
            position = await producer.publish(
                Event.create(
                    "cluster-events",
                    f"cluster-key-{index}",
                    f"event-{index}".encode(),
                )
            )
            expected_offsets[position.partition_id] = position.offset + 1

        processed_counts = []
        for index in range(4):
            worker_id = f"cluster-worker-{index}"
            code = f"""
import asyncio
from meshflow.worker import Worker
async def main():
    processed = []
    worker = Worker(
        {worker_id!r}, "cluster-group", ["cluster-events"],
        coordinator_host={coordinator_host!r},
        coordinator_port={coordinator_port},
        handler=lambda event: processed.append(event.event_id),
    )
    print(await worker.run_once())
asyncio.run(main())
"""
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-c",
                code,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=environment,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=15)
            assert process.returncode == 0, stdout.decode(
                "utf-8", errors="replace"
            ) + stderr.decode("utf-8", errors="replace")
            processed_counts.append(int(stdout.decode().strip().splitlines()[-1]))

        assert sum(processed_counts) == 36
        for partition_id, expected_offset in enumerate(expected_offsets):
            assignment = next(item for item in assignments if item["partition_id"] == partition_id)
            worker_consumer = Consumer(
                "127.0.0.1",
                9500,
                group_id="cluster-group",
                sender_id=assignment["worker_id"],
                coordinator_host=coordinator_host,
                coordinator_port=coordinator_port,
            )
            result = await worker_consumer.fetch(
                "cluster-events",
                partition_id,
                assignment_epoch=assignment["epoch"],
            )
            assert result.committed_offset == expected_offset
    finally:
        for broker in brokers:
            await broker.close()
        await coordinator.close()
