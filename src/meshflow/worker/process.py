"""Worker registration, assigned-partition polling, and offset commits."""

from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import math
import random
import time
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from statistics import quantiles
from uuid import uuid4

from meshflow.common.metrics import ProcessMetricsCollector
from meshflow.common.types import Event
from meshflow.consumer import Consumer
from meshflow.coordinator.client import CoordinatorClient
from meshflow.producer import Producer
from meshflow.protocol.client import BrokerRequestError
from meshflow.protocol.messages import ProtocolError

logger = logging.getLogger(__name__)
EventHandler = Callable[[Event], None | Awaitable[None]]


class PermanentHandlerError(Exception):
    """Mark a handler failure as permanent so it is not retried."""


class _HandlerFailureError(Exception):
    def __init__(self, error: Exception, attempts: int) -> None:
        super().__init__(str(error))
        self.error = error
        self.attempts = attempts


class Worker:
    """Runs a consumer group worker against coordinator-assigned partitions."""

    def __init__(
        self,
        worker_id: str,
        group_id: str,
        topics: Sequence[str],
        *,
        coordinator_host: str = "127.0.0.1",
        coordinator_port: int = 9501,
        capacity: float = 1.0,
        batch_size: int = 100,
        poll_interval: float = 0.25,
        heartbeat_interval: float = 5.0,
        handler: EventHandler | None = None,
        max_handler_retries: int = 3,
        retry_backoff_base: float = 0.05,
        dead_letter_topic_suffix: str | None = ".DLQ",
    ) -> None:
        if not worker_id or len(worker_id) > 128:
            raise ValueError("worker_id must contain 1 to 128 characters")
        if not group_id or len(group_id) > 128:
            raise ValueError("group_id must contain 1 to 128 characters")
        if not topics or any(not topic for topic in topics):
            raise ValueError("topics must be a non-empty sequence")
        if (
            isinstance(capacity, bool)
            or not isinstance(capacity, (int, float))
            or not math.isfinite(capacity)
            or capacity <= 0
            or isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size <= 0
            or not isinstance(poll_interval, (int, float))
            or isinstance(poll_interval, bool)
            or not math.isfinite(poll_interval)
            or poll_interval <= 0
            or not isinstance(heartbeat_interval, (int, float))
            or isinstance(heartbeat_interval, bool)
            or not math.isfinite(heartbeat_interval)
            or heartbeat_interval <= 0
            or isinstance(max_handler_retries, bool)
            or not isinstance(max_handler_retries, int)
            or max_handler_retries < 0
            or not isinstance(retry_backoff_base, (int, float))
            or isinstance(retry_backoff_base, bool)
            or not math.isfinite(retry_backoff_base)
            or retry_backoff_base < 0
        ):
            raise ValueError(
                "worker limits must be positive and max_handler_retries/backoff non-negative"
            )
        if dead_letter_topic_suffix is not None and (
            not dead_letter_topic_suffix or len(dead_letter_topic_suffix) > 64
        ):
            raise ValueError("dead_letter_topic_suffix must contain 1 to 64 characters or None")
        self.worker_id = worker_id
        self.group_id = group_id
        self.topics = list(dict.fromkeys(topics))
        self.capacity = float(capacity)
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.max_handler_retries = max_handler_retries
        self.retry_backoff_base = retry_backoff_base
        self.dead_letter_topic_suffix = dead_letter_topic_suffix
        self._coordinator_host = coordinator_host
        self._coordinator_port = coordinator_port
        self._created_dead_letter_topics: set[str] = set()
        self._handler = handler or self._log_event
        self._coordinator = CoordinatorClient(
            coordinator_host,
            coordinator_port,
            sender_id=worker_id,
        )
        self._registered = False
        self._stop = asyncio.Event()
        self._metrics_collector = ProcessMetricsCollector()
        self._instance_id = str(uuid4())
        self._latency_samples_ms: deque[float] = deque(maxlen=256)
        self._last_metrics_time = time.monotonic()

    async def register(self) -> None:
        """Register this worker identity with the coordinator."""
        await self._coordinator.register_node(
            self.worker_id, "worker", "127.0.0.1", 0, capacity=self.capacity
        )
        await self._coordinator.heartbeat(self.worker_id)
        self._registered = True

    async def run_once(self) -> int:
        """Join/refresh the group, process one batch per assigned partition."""
        if not self._registered:
            await self.register()
        else:
            await self._coordinator.heartbeat(self.worker_id)
        assignments = await self._coordinator.join_group(
            self.group_id,
            self.worker_id,
            self.topics,
            capacity=self.capacity,
        )
        started = time.monotonic()
        processed = 0
        queued_peak = 0
        in_flight = 0
        partition_lags: list[dict[str, object]] = []
        for assignment in assignments:
            if assignment.get("worker_id") != self.worker_id:
                continue
            topic = assignment.get("topic")
            partition_id = assignment.get("partition_id")
            if (
                not isinstance(topic, str)
                or isinstance(partition_id, bool)
                or not isinstance(partition_id, int)
            ):
                raise RuntimeError("Coordinator returned an invalid assignment")
            epoch = assignment.get("epoch")
            state = assignment.get("state")
            if (
                isinstance(epoch, bool)
                or not isinstance(epoch, int)
                or epoch <= 0
                or state not in {"ACTIVE", "REVOKING", "UNASSIGNED"}
            ):
                raise RuntimeError("Coordinator returned an invalid assignment state")
            if state == "REVOKING":
                await self._coordinator.acknowledge_revocation(
                    self.group_id, self.worker_id, topic, partition_id, epoch
                )
                continue
            if state == "UNASSIGNED":
                continue
            broker_host = assignment.get("broker_host")
            broker_port = assignment.get("broker_port")
            if (
                not isinstance(broker_host, str)
                or isinstance(broker_port, bool)
                or not isinstance(broker_port, int)
                or not 0 <= broker_port <= 65535
            ):
                raise RuntimeError("Coordinator returned an invalid broker endpoint")
            consumer = Consumer(
                broker_host,
                broker_port,
                group_id=self.group_id,
                sender_id=self.worker_id,
            )
            try:
                result = await consumer.fetch(
                    topic,
                    partition_id,
                    max_records=self.batch_size,
                    assignment_epoch=epoch,
                )
            except BrokerRequestError as exc:
                if exc.code != "ASSIGNMENT_REVOKING":
                    raise
                await self._coordinator.acknowledge_revocation(
                    self.group_id, self.worker_id, topic, partition_id, epoch
                )
                continue
            queued_peak = max(queued_peak, len(result.records))
            completed = result.committed_offset
            for record in result.records:
                in_flight += 1
                process_started = time.perf_counter()
                event = record.to_event(topic)
                try:
                    await self._process_with_retries(event)
                except _HandlerFailureError as failure:
                    if self.dead_letter_topic_suffix is None:
                        raise failure.error
                    await self._publish_dead_letter(
                        event,
                        partition_id=partition_id,
                        offset=record.offset,
                        error=failure.error,
                        attempts=failure.attempts,
                    )
                self._latency_samples_ms.append((time.perf_counter() - process_started) * 1000)
                in_flight -= 1
                completed = record.offset + 1
                processed += 1
            if completed > result.committed_offset:
                await consumer.commit(topic, partition_id, completed, assignment_epoch=epoch)
            partition_lags.append(
                {
                    "group_id": self.group_id,
                    "topic": topic,
                    "partition_id": partition_id,
                    "lag_records": max(0, result.next_offset - completed),
                }
            )
        elapsed = max(time.monotonic() - started, 0.001)
        interval_ms = max(0, int((time.monotonic() - self._last_metrics_time) * 1000))
        self._last_metrics_time = time.monotonic()
        process_metrics = self._metrics_collector.sample()
        p95 = (
            quantiles(self._latency_samples_ms, n=20)[18]
            if len(self._latency_samples_ms) >= 2
            else (self._latency_samples_ms[0] if self._latency_samples_ms else 0.0)
        )
        await self._coordinator.report_metrics(
            self._instance_id,
            interval_ms,
            {
                "group_id": self.group_id,
                "cpu_percent": process_metrics.cpu_percent,
                "memory_percent": process_metrics.memory_percent,
                "queue_depth": queued_peak,
                "queue_capacity": self.batch_size,
                "p95_latency_ms": p95,
                "processing_rate": processed / elapsed,
                "in_flight": in_flight,
                "partition_lags": partition_lags,
            },
        )
        return processed

    async def run(self) -> None:
        """Poll until stop() is called."""
        self._stop.clear()
        await self.register()
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        stopped_cleanly = False
        try:
            while not self._stop.is_set():
                processed = await self.run_once()
                if processed == 0:
                    try:
                        await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
                    except TimeoutError:
                        pass
            stopped_cleanly = True
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
            if stopped_cleanly:
                await self._coordinator.leave_group(self.group_id, self.worker_id)

    def stop(self) -> None:
        self._stop.set()

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            try:
                await self._coordinator.heartbeat(self.worker_id)
            except (BrokerRequestError, OSError, TimeoutError, ProtocolError) as exc:
                logger.warning(
                    "Worker heartbeat failed: worker_id=%s error=%s", self.worker_id, exc
                )

    async def _process(self, event: Event) -> None:
        result = self._handler(event)
        if inspect.isawaitable(result):
            await result

    async def _process_with_retries(self, event: Event) -> None:
        for attempt in range(self.max_handler_retries + 1):
            try:
                await self._process(event)
                return
            except PermanentHandlerError as exc:
                raise _HandlerFailureError(exc, attempt + 1) from exc
            except Exception as exc:
                if attempt >= self.max_handler_retries:
                    raise _HandlerFailureError(exc, attempt + 1) from exc
                logger.warning(
                    "Event handler failed; retrying: worker_id=%s event_id=%s "
                    "attempt=%d max_attempts=%d error=%s",
                    self.worker_id,
                    event.event_id,
                    attempt + 1,
                    self.max_handler_retries + 1,
                    exc,
                )
                delay = min(self.retry_backoff_base * (2**attempt), 2.0)
                if delay:
                    await asyncio.sleep(min(random.uniform(0.5, 1.5) * delay, 2.0))

    async def _publish_dead_letter(
        self,
        event: Event,
        *,
        partition_id: int,
        offset: int,
        error: Exception,
        attempts: int,
    ) -> None:
        suffix = self.dead_letter_topic_suffix
        if suffix is None:
            raise RuntimeError("Dead-letter publishing is disabled")
        topic = f"{event.topic}{suffix}"
        if len(topic) > 128:
            raise ValueError("Dead-letter topic name exceeds 128 characters")
        if topic not in self._created_dead_letter_topics:
            await self._coordinator.create_topic(topic, 1)
            self._created_dead_letter_topics.add(topic)
        dead_letter_event = Event(
            event_id=event.event_id,
            topic=topic,
            partition_key=event.partition_key,
            timestamp_ms=event.timestamp_ms,
            schema_version=event.schema_version,
            payload=event.payload,
            headers={
                **event.headers,
                "meshflow.original_topic": event.topic,
                "meshflow.original_partition": str(partition_id),
                "meshflow.original_offset": str(offset),
                "meshflow.failure_attempts": str(attempts),
                "meshflow.failure_timestamp_ms": str(int(time.time() * 1000)),
                "meshflow.failure": str(error)[:2048],
            },
        )
        logger.error(
            "Sending event to dead-letter topic: worker_id=%s event_id=%s source=%s/%d/%d",
            self.worker_id,
            event.event_id,
            event.topic,
            partition_id,
            offset,
        )
        producer = Producer(
            "127.0.0.1",
            0,
            sender_id=self.worker_id,
            coordinator_host=self._coordinator_host,
            coordinator_port=self._coordinator_port,
        )
        await producer.publish(dead_letter_event)

    @staticmethod
    async def _log_event(event: Event) -> None:
        logger.info(
            "Processed event id=%s topic=%s payload_bytes=%d",
            event.event_id,
            event.topic,
            len(event.payload),
        )


async def _run(args: argparse.Namespace) -> None:
    worker = Worker(
        args.worker_id,
        args.group_id,
        args.topics,
        coordinator_host=args.coordinator_host,
        coordinator_port=args.coordinator_port,
        capacity=args.capacity,
        batch_size=args.batch_size,
        poll_interval=args.poll_interval,
        heartbeat_interval=args.heartbeat_interval,
        max_handler_retries=args.max_handler_retries,
        retry_backoff_base=args.retry_backoff_base,
        dead_letter_topic_suffix=None if args.no_dead_letter else args.dead_letter_suffix,
    )
    await worker.run()


def main() -> None:
    """Run a development worker process."""
    parser = argparse.ArgumentParser(description="Run a MeshFlow consumer worker")
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--topics", nargs="+", required=True)
    parser.add_argument("--coordinator-host", default="127.0.0.1")
    parser.add_argument("--coordinator-port", type=int, default=9501)
    parser.add_argument("--capacity", type=float, default=1.0)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--poll-interval", type=float, default=0.25)
    parser.add_argument("--heartbeat-interval", type=float, default=5.0)
    parser.add_argument("--max-handler-retries", type=int, default=3)
    parser.add_argument("--retry-backoff-base", type=float, default=0.05)
    parser.add_argument("--dead-letter-suffix", default=".DLQ")
    parser.add_argument("--no-dead-letter", action="store_true")
    asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    main()
