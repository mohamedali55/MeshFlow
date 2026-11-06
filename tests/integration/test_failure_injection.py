from __future__ import annotations

import asyncio
import errno
import sys

import pytest

from meshflow.benchmark.failures import FailureInjector
from meshflow.benchmark.live import run_tcp_benchmark
from meshflow.broker.segment import LogSegment
from meshflow.broker.server import BrokerServer
from meshflow.common.config import BrokerConfig
from meshflow.common.types import Event
from meshflow.producer import Producer
from meshflow.protocol.client import BrokerRequestError


@pytest.mark.asyncio
async def test_network_delay_and_child_process_termination() -> None:
    async def operation(value: str) -> str:
        return value

    started = asyncio.get_running_loop().time()
    result = await FailureInjector.with_network_delay(0.01, operation, "delayed")
    assert result == "delayed"
    assert asyncio.get_running_loop().time() - started >= 0.01

    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import time; time.sleep(30)",
    )
    return_code = await asyncio.wait_for(FailureInjector.terminate_process(process), timeout=5)
    assert return_code != 0


@pytest.mark.asyncio
async def test_corrupted_log_failure_injection_is_detected_on_recovery(tmp_path) -> None:
    config = BrokerConfig(broker_id="failure-broker", data_dir=tmp_path, port=0)
    broker = BrokerServer(config)
    await broker.start()
    host, port = broker.address
    try:
        await Producer(host, port).publish(Event.create("events", "key", b"payload"))
    finally:
        await broker.close()

    segment_path = next(config.partition_dir("events", 0).glob("segment-*.log"))
    FailureInjector.corrupt_log(
        segment_path,
        root=tmp_path,
        offset=segment_path.stat().st_size - 1,
    )
    recovered = BrokerServer(config)
    await recovered.start()
    try:
        assert ("events", 0) in recovered._damaged_partitions
    finally:
        await recovered.close()


@pytest.mark.asyncio
async def test_live_tcp_benchmark_reports_measured_publish_and_consume() -> None:
    report = await run_tcp_benchmark(
        scenario="hot_partition",
        events=24,
        partitions=4,
        workers=2,
        seed=6,
        payload_bytes=8,
    )
    sample = report["raw_runs"][0]
    assert report["benchmark_type"] == "local_tcp_broker_measurement"
    assert report["configuration"]["platform"]
    assert report["configuration"]["python"]
    assert sample["published_events"] == 24
    assert sample["consumed_events"] == 24
    assert sample["lost_events"] == 0
    assert sample["estimated_lag_records"] == 0
    assert sample["publish_p95_latency_ms"] >= sample["publish_p50_latency_ms"]


@pytest.mark.asyncio
async def test_simulated_disk_full_marks_partition_read_only(tmp_path, monkeypatch) -> None:
    config = BrokerConfig(broker_id="disk-full-broker", data_dir=tmp_path, port=0)
    broker = BrokerServer(config)
    await broker.start()
    host, port = broker.address

    def fail_append(self, record):
        raise OSError(errno.ENOSPC, "simulated disk is full")

    monkeypatch.setattr(LogSegment, "append", fail_append)
    producer = Producer(host, port)
    try:
        with pytest.raises(BrokerRequestError) as failure:
            await producer.publish(Event.create("events", "key", b"cannot-write"))
        assert failure.value.code == "DISK_FULL"
        assert broker._logs[("events", 0)].status.value == "readonly"
    finally:
        await broker.close()
