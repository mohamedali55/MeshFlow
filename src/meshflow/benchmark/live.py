"""Local TCP broker measurements using the public producer and consumer APIs."""

from __future__ import annotations

import asyncio
import os
import platform
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import psutil

from meshflow.benchmark.runner import _percentile
from meshflow.benchmark.workloads import generate_workload
from meshflow.broker.server import BrokerServer
from meshflow.common.config import BrokerConfig
from meshflow.common.hashing import select_partition
from meshflow.common.types import Event
from meshflow.consumer import Consumer
from meshflow.producer import Producer


async def run_tcp_benchmark(
    *,
    scenario: str = "uniform",
    events: int = 1_000,
    partitions: int = 8,
    workers: int = 1,
    seed: int = 1,
    payload_bytes: int = 128,
) -> dict[str, Any]:
    """Measure publish/fetch/commit over real TCP sockets against a temporary broker."""
    workload = generate_workload(
        scenario,
        partitions=partitions,
        workers=workers,
        events=events,
        seed=seed,
    )
    if events <= 0:
        raise ValueError("events must be positive for a TCP benchmark")
    if isinstance(payload_bytes, bool) or not isinstance(payload_bytes, int) or payload_bytes < 0:
        raise ValueError("payload_bytes must be a non-negative integer")

    with tempfile.TemporaryDirectory(prefix="meshflow-benchmark-") as data_dir:
        broker = BrokerServer(
            BrokerConfig(
                broker_id="benchmark-broker",
                data_dir=Path(data_dir),
                port=0,
            ),
            topic_partitions={"benchmark": partitions, "benchmark-warmup": 1},
            max_connections=64,
        )
        await broker.start()
        try:
            host, port = broker.address
            producer = Producer(
                host,
                port,
                sender_id="benchmark-producer",
                partition_counts={"benchmark": partitions, "benchmark-warmup": 1},
                max_in_flight=32,
            )
            await producer.publish(Event.create("benchmark-warmup", "warmup", b"warmup"))

            keys: dict[int, str] = {}
            for partition_id in range(partitions):
                candidate = 0
                while True:
                    key = f"meshflow-benchmark-{partition_id}-{candidate}"
                    if select_partition(key, partitions) == partition_id:
                        keys[partition_id] = key
                        break
                    candidate += 1

            latency_samples: list[float] = []
            process = psutil.Process()
            cpu_before = process.cpu_percent(interval=None)
            memory_before = process.memory_percent()
            publish_started = time.perf_counter()
            for partition_id, count in enumerate(workload.partition_events):
                for batch_start in range(0, count, 128):
                    batch_end = min(batch_start + 128, count)
                    batch = [
                        Event.create(
                            "benchmark",
                            keys[partition_id],
                            b"x" * payload_bytes,
                        )
                        for _ in range(batch_start, batch_end)
                    ]

                    async def publish_timed(event: Event) -> None:
                        started = time.perf_counter()
                        await producer.publish(event)
                        latency_samples.append((time.perf_counter() - started) * 1000)

                    await asyncio.gather(*(publish_timed(event) for event in batch))
            publish_seconds = max(time.perf_counter() - publish_started, 1e-9)

            consumer = Consumer(host, port, group_id="benchmark-consumer")
            consume_started = time.perf_counter()
            consumed = 0
            lag = 0
            seen_event_ids: set[str] = set()
            duplicate_events = 0
            for partition_id in range(partitions):
                offset = 0
                while True:
                    result = await consumer.fetch(
                        "benchmark",
                        partition_id,
                        offset=offset,
                        max_records=1000,
                    )
                    if not result.records:
                        lag += max(0, result.next_offset - offset)
                        break
                    consumed += len(result.records)
                    for record in result.records:
                        if record.event_id in seen_event_ids:
                            duplicate_events += 1
                        else:
                            seen_event_ids.add(record.event_id)
                    offset = result.records[-1].offset + 1
                    await consumer.commit("benchmark", partition_id, offset)
                    lag += max(0, result.next_offset - offset)
            consume_seconds = max(time.perf_counter() - consume_started, 1e-9)
            cpu_after = process.cpu_percent(interval=None)
            memory_after = process.memory_percent()
            resource_sample = process.memory_info()

            return {
                "benchmark_type": "local_tcp_broker_measurement",
                "interpretation": (
                    "Measured against a local single broker; results do not represent "
                    "multi-process cluster or production hardware performance."
                ),
                "configuration": {
                    "scenario": scenario,
                    "events": events,
                    "partitions": partitions,
                    "workers_in_scenario": workers,
                    "seed": seed,
                    "payload_bytes": payload_bytes,
                    "warmup_events": 1,
                    "broker_count": 1,
                    "python": sys.version.split()[0],
                    "platform": platform.platform(),
                    "logical_cpu_count": os.cpu_count(),
                },
                "raw_runs": [
                    {
                        "scenario": scenario,
                        "seed": seed,
                        "partitions": partitions,
                        "workers_in_scenario": workers,
                        "payload_bytes": payload_bytes,
                        "published_events": events,
                        "consumed_events": consumed,
                        "lost_events": max(0, events - consumed),
                        "duplicate_events": duplicate_events,
                        "estimated_lag_records": lag,
                        "publish_throughput_events_per_second": events / publish_seconds,
                        "consume_throughput_events_per_second": consumed / consume_seconds,
                        "publish_p50_latency_ms": _percentile(latency_samples, 0.50),
                        "publish_p95_latency_ms": _percentile(latency_samples, 0.95),
                        "publish_p99_latency_ms": _percentile(latency_samples, 0.99),
                        "runner_process_cpu_percent_delta": cpu_after - cpu_before,
                        "runner_process_memory_percent_delta": memory_after - memory_before,
                        "process_rss_bytes": resource_sample.rss,
                        "process_vms_bytes": resource_sample.vms,
                        "publish_duration_seconds": publish_seconds,
                        "consume_duration_seconds": consume_seconds,
                        "latency_samples": len(latency_samples),
                    }
                ],
            }
        finally:
            await broker.close()
