"""Asynchronous TCP broker for publish, fetch, and consumer offset commits."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import errno
import json
import logging
import math
import os
import re
import time
from collections import deque
from collections.abc import Mapping
from pathlib import Path
from statistics import quantiles
from typing import Any
from uuid import uuid4

from meshflow.broker.recovery import BrokerRecovery
from meshflow.broker.serialization import Record
from meshflow.broker.storage import PartitionLog
from meshflow.common.config import BrokerConfig
from meshflow.common.hashing import select_partition
from meshflow.common.metrics import ProcessMetricsCollector
from meshflow.common.types import Event, PartitionStatus
from meshflow.coordinator.client import CoordinatorClient
from meshflow.protocol.client import BrokerClient, BrokerRequestError
from meshflow.protocol.messages import (
    Envelope,
    MessageType,
    ProtocolError,
    read_envelope,
    write_envelope,
)

logger = logging.getLogger(__name__)
_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
MAX_CHECKPOINT_BYTES = 1024 * 1024


class RequestValidationError(Exception):
    """Raised when a request payload is invalid."""


class BrokerOperationError(Exception):
    """Raised when a broker request cannot be completed."""

    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class BrokerServer:
    """Owns partition logs and serves the MeshFlow TCP protocol."""

    def __init__(
        self,
        config: BrokerConfig,
        *,
        topic_partitions: Mapping[str, int] | None = None,
        default_partition_count: int = 1,
        connection_timeout: float = 10.0,
        max_connections: int = 100,
        coordinator_host: str | None = None,
        coordinator_port: int = 9501,
        heartbeat_interval: float = 5.0,
    ) -> None:
        if default_partition_count <= 0:
            raise ValueError("default_partition_count must be positive")
        if (
            not math.isfinite(connection_timeout)
            or connection_timeout <= 0
            or not math.isfinite(heartbeat_interval)
            or heartbeat_interval <= 0
        ):
            raise ValueError("connection_timeout and heartbeat_interval must be positive")
        if max_connections <= 0:
            raise ValueError("max_connections must be positive")
        self._config = config
        if coordinator_host is not None and not 0 <= coordinator_port <= 65535:
            raise ValueError("coordinator_port must be between 0 and 65535")
        self._coordinator_host = coordinator_host
        self._coordinator_port = coordinator_port
        self._heartbeat_interval = heartbeat_interval
        self._coordinator: CoordinatorClient | None = None
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._topic_partitions = dict(topic_partitions or {})
        for topic, count in self._topic_partitions.items():
            self._validate_name(topic, "topic")
            if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
                raise ValueError(f"partition count for topic {topic!r} must be positive")
        self._default_partition_count = default_partition_count
        self._connection_timeout = connection_timeout
        self._max_connections = max_connections
        self._logs: dict[tuple[str, int], PartitionLog] = {}
        self._damaged_partitions: set[tuple[str, int]] = set()
        self._partition_locks: dict[tuple[str, int], asyncio.Lock] = {}
        self._committed_offsets: dict[tuple[str, str, int], int] = {}
        self._checkpoints: dict[tuple[str, str, int], tuple[int, bytes]] = {}
        self._offset_lock = asyncio.Lock()
        self._server: asyncio.Server | None = None
        self._connection_tasks: set[asyncio.Task[None]] = set()
        self._active_connections = 0
        self._metrics_collector = ProcessMetricsCollector()
        self._instance_id = str(uuid4())
        self._request_latency_ms: deque[float] = deque(maxlen=512)
        self._requests_since_metrics = 0
        self._last_metrics_time = time.monotonic()

    @property
    def address(self) -> tuple[str, int]:
        """Bound host and port; available after start()."""
        if self._server is None or not self._server.sockets:
            raise RuntimeError("Broker server is not started")
        address = self._server.sockets[0].getsockname()
        return str(address[0]), int(address[1])

    async def start(self) -> None:
        """Recover local logs and begin listening for TCP connections."""
        if self._server is not None:
            raise RuntimeError("Broker server is already started")

        self._load_committed_offsets()
        recovery = BrokerRecovery(self._config)
        result = await asyncio.to_thread(recovery.recover)
        self._logs = recovery.partition_logs
        self._damaged_partitions = {
            (partition.topic, partition.partition_id)
            for partition in result.partitions
            if partition.status == PartitionStatus.DAMAGED
        }
        self._server = await asyncio.start_server(
            self._handle_connection,
            self._config.host,
            self._config.port,
            start_serving=True,
        )
        if self._coordinator_host is not None:
            try:
                host, port = self.address
                client = BrokerClient(
                    self._coordinator_host,
                    self._coordinator_port,
                    sender_id=self._config.broker_id,
                    timeout=self._connection_timeout,
                )
                response = await client.request(
                    MessageType.REGISTER_NODE,
                    {
                        "node_id": self._config.broker_id,
                        "role": "broker",
                        "host": host,
                        "port": port,
                    },
                )
                if response.message_type != MessageType.NODE_STATUS:
                    raise RuntimeError(
                        "Coordinator returned an invalid broker registration response"
                    )
                self._coordinator = CoordinatorClient(
                    self._coordinator_host,
                    self._coordinator_port,
                    sender_id=self._config.broker_id,
                    timeout=self._connection_timeout,
                )
                await self._coordinator.heartbeat(self._config.broker_id)
            except (OSError, TimeoutError, RuntimeError, BrokerRequestError, ProtocolError):
                server = self._server
                self._server = None
                server.close()
                await server.wait_closed()
                logs = list(self._logs.values())
                self._logs.clear()
                if logs:
                    await asyncio.gather(*(asyncio.to_thread(log.close) for log in logs))
                raise
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("Broker %s listening on %s:%d", self._config.broker_id, *self.address)

    async def serve_forever(self) -> None:
        """Serve until cancelled or stopped."""
        if self._server is None:
            await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def close(self) -> None:
        """Stop accepting connections and close recovered partition logs."""
        heartbeat_task = self._heartbeat_task
        self._heartbeat_task = None
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            await server.wait_closed()

        current = asyncio.current_task()
        pending = [task for task in self._connection_tasks if task is not current]
        if pending:
            await asyncio.gather(*pending)

        logs = list(self._logs.values())
        self._logs.clear()
        if logs:
            await asyncio.gather(*(asyncio.to_thread(log.close) for log in logs))

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connection_tasks.add(task)
        self._active_connections += 1
        request_started = time.perf_counter()
        try:
            try:
                request = await read_envelope(
                    reader,
                    max_frame_bytes=self._config.max_frame_bytes,
                    timeout=self._connection_timeout,
                )
            except (ProtocolError, OSError) as exc:
                logger.warning("Rejected invalid broker frame: %s", exc)
                return

            if request.deadline_ms and request.deadline_ms < int(time.time() * 1000):
                response = self._error_response(
                    request, "DEADLINE_EXCEEDED", "Request deadline has expired"
                )
            elif self._active_connections > self._max_connections:
                response = self._overloaded_response(request)
            else:
                response = await self._dispatch(request)
            try:
                await write_envelope(
                    writer,
                    response,
                    max_frame_bytes=self._config.max_frame_bytes,
                    timeout=self._connection_timeout,
                )
            except (ProtocolError, OSError) as exc:
                logger.warning("Failed to send broker response: %s", exc)
        finally:
            self._request_latency_ms.append((time.perf_counter() - request_started) * 1000)
            self._requests_since_metrics += 1
            self._active_connections -= 1
            if task is not None:
                self._connection_tasks.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                logger.debug("Socket closed with an error during graceful shutdown", exc_info=True)

    async def _dispatch(self, request: Envelope) -> Envelope:
        try:
            if request.message_type == MessageType.PUBLISH:
                return await self._publish(request)
            if request.message_type == MessageType.REPLICATE_APPEND:
                return await self._replicate_append(request)
            if request.message_type == MessageType.FETCH:
                return await self._fetch(request)
            if request.message_type == MessageType.COMMIT_OFFSET:
                return await self._commit_offset(request)
            return self._error_response(
                request,
                "UNSUPPORTED_MESSAGE",
                f"Message type {request.message_type.value} is not served by a broker",
            )
        except RequestValidationError as exc:
            return self._error_response(request, "INVALID_REQUEST", str(exc))
        except BrokerOperationError as exc:
            return self._error_response(request, exc.code, str(exc), retryable=exc.retryable)
        except BrokerRequestError as exc:
            if exc.code in {
                "STALE_EPOCH",
                "ASSIGNMENT_REVOKING",
                "BROKER_NOT_HEALTHY",
                "REPLICATION_UNAVAILABLE",
                "NOT_REPLICA",
            }:
                return self._error_response(request, exc.code, str(exc))
            return self._error_response(
                request,
                "COORDINATOR_UNAVAILABLE",
                str(exc),
                retryable=True,
            )
        except OSError as exc:
            logger.exception(
                "Broker storage operation failed: type=%s request_id=%s",
                request.message_type.value,
                request.request_id,
            )
            if exc.errno == errno.ENOSPC:
                return self._error_response(request, "DISK_FULL", "Broker storage is full")
            return self._error_response(request, "STORAGE_ERROR", "Broker storage operation failed")
        except (RuntimeError, ValueError):
            logger.exception(
                "Broker request failed: type=%s request_id=%s",
                request.message_type.value,
                request.request_id,
            )
            return self._error_response(
                request, "BROKER_ERROR", "Broker could not complete request"
            )

    async def _publish(self, request: Envelope) -> Envelope:
        payload = request.payload
        topic = self._required_string(payload, "topic")
        self._validate_name(topic, "topic")
        partition_id = self._required_int(payload, "partition_id", minimum=0)
        event_data = payload.get("event")
        if not isinstance(event_data, dict):
            raise RequestValidationError("event must be a JSON object")
        event = self._parse_event(topic, event_data)
        partition_count = self._partition_count(topic)
        if self._coordinator is not None:
            route = await self._coordinator.partition_route(topic, event.partition_key)
            route_count = route.get("partition_count")
            route_partition = route.get("partition_id")
            route_broker = route.get("broker_id")
            if (
                isinstance(route_count, bool)
                or not isinstance(route_count, int)
                or isinstance(route_partition, bool)
                or not isinstance(route_partition, int)
                or not isinstance(route_broker, str)
            ):
                raise RequestValidationError("Coordinator returned an invalid partition route")
            partition_count = route_count
            if route_broker != self._config.broker_id:
                raise BrokerOperationError(
                    "NOT_LEADER",
                    f"Partition is owned by broker {route_broker!r}",
                    retryable=True,
                )
            if partition_id != route_partition:
                raise RequestValidationError(
                    f"partition_id {partition_id} does not match coordinator partition "
                    f"{route_partition}"
                )
        if partition_id >= partition_count:
            raise RequestValidationError(
                f"partition_id must be less than topic partition count {partition_count}"
            )
        expected_partition = select_partition(event.partition_key, partition_count)
        if partition_id != expected_partition:
            raise RequestValidationError(
                f"partition_id {partition_id} does not match key-selected partition "
                f"{expected_partition}"
            )

        key = (topic, partition_id)
        lock = self._partition_locks.setdefault(key, asyncio.Lock())
        async with lock:
            log = await self._get_log(key, create=True)
            assert log is not None
            if log.status == PartitionStatus.DAMAGED:
                raise BrokerOperationError("PARTITION_DAMAGED", "Partition is damaged")
            if log.status == PartitionStatus.READONLY:
                raise BrokerOperationError("PARTITION_READONLY", "Partition is read-only")
            try:
                offset, appended = await asyncio.to_thread(log.append_once, event)
            except ValueError as exc:
                raise RequestValidationError(str(exc)) from exc
            replicas = route.get("replicas", []) if self._coordinator is not None else []
            if not isinstance(replicas, list):
                raise RequestValidationError("Coordinator returned invalid replica metadata")
            for replica in replicas:
                if not isinstance(replica, dict):
                    raise RequestValidationError("Coordinator returned invalid replica metadata")
                replica_id = replica.get("broker_id")
                if replica_id == self._config.broker_id:
                    continue
                replica_host = replica.get("broker_host")
                replica_port = replica.get("broker_port")
                if (
                    not isinstance(replica_host, str)
                    or isinstance(replica_port, bool)
                    or not isinstance(replica_port, int)
                ):
                    raise RequestValidationError("Coordinator returned an invalid replica route")
                follower = BrokerClient(
                    replica_host,
                    replica_port,
                    sender_id=self._config.broker_id,
                    timeout=self._connection_timeout,
                    max_retries=0,
                )
                try:
                    response = await follower.request(
                        MessageType.REPLICATE_APPEND,
                        {
                            "topic": topic,
                            "partition_id": partition_id,
                            "leader_id": self._config.broker_id,
                            "offset": offset,
                            "event": event_data,
                        },
                    )
                except (BrokerRequestError, OSError, TimeoutError, ProtocolError) as exc:
                    raise BrokerOperationError(
                        "REPLICATION_UNAVAILABLE",
                        f"Follower {replica_id!r} is unavailable: {exc}",
                        retryable=True,
                    ) from exc
                if response.message_type != MessageType.PUBLISH_ACK:
                    raise BrokerOperationError(
                        "REPLICATION_UNAVAILABLE",
                        f"Follower {replica_id!r} did not acknowledge replication",
                        retryable=True,
                    )
        return self._response(
            request,
            MessageType.PUBLISH_ACK,
            {
                "topic": topic,
                "partition_id": partition_id,
                "offset": offset,
                "duplicate": not appended,
            },
        )

    async def _replicate_append(self, request: Envelope) -> Envelope:
        payload = request.payload
        topic = self._required_string(payload, "topic")
        self._validate_name(topic, "topic")
        partition_id = self._required_int(payload, "partition_id", minimum=0)
        expected_offset = self._required_int(payload, "offset", minimum=0)
        leader_id = self._required_string(payload, "leader_id")
        if self._coordinator is not None:
            route = await self._coordinator.partition_by_id(topic, partition_id)
            if route.get("broker_id") != leader_id:
                raise BrokerOperationError("NOT_LEADER", "Replication source is not the leader")
            replicas = route.get("replicas", [])
            if not any(
                isinstance(item, dict) and item.get("broker_id") == self._config.broker_id
                for item in replicas
            ):
                raise BrokerOperationError("NOT_REPLICA", "Broker is not a configured replica")
        event_data = payload.get("event")
        if not isinstance(event_data, dict):
            raise RequestValidationError("event must be a JSON object")
        event = self._parse_event(topic, event_data)
        key = (topic, partition_id)
        lock = self._partition_locks.setdefault(key, asyncio.Lock())
        async with lock:
            log = await self._get_log(key, create=True)
            assert log is not None
            offset, _ = await asyncio.to_thread(log.append_replica, event, expected_offset)
        return self._response(
            request,
            MessageType.PUBLISH_ACK,
            {"topic": topic, "partition_id": partition_id, "offset": offset, "duplicate": True},
        )

    async def _fetch(self, request: Envelope) -> Envelope:
        payload = request.payload
        topic = self._required_string(payload, "topic")
        self._validate_name(topic, "topic")
        partition_id = self._required_int(payload, "partition_id", minimum=0)
        partition_count = await self._topic_partition_count(topic)
        if partition_id >= partition_count:
            raise RequestValidationError("partition_id is outside the topic partition range")
        await self._validate_partition_leader(topic, partition_id)
        max_records = self._required_int(payload, "max_records", minimum=1, default=100)
        if max_records > 1000:
            raise RequestValidationError("max_records must not exceed 1000")

        group_id = payload.get("group_id")
        if self._coordinator is not None and group_id is None:
            raise RequestValidationError("group_id is required for fetches on a coordinated broker")
        if group_id is not None:
            if not isinstance(group_id, str):
                raise RequestValidationError("group_id must be a string")
            self._validate_name(group_id, "group_id")
            await self._authorize_group_request(request, group_id, topic, partition_id, "FETCH")

        key = (topic, partition_id)
        lock = self._partition_locks.setdefault(key, asyncio.Lock())
        async with lock:
            log = await self._get_log(key, create=False)
            committed = (
                self._committed_offsets.get((group_id, topic, partition_id), 0) if group_id else 0
            )
            offset = payload.get("offset", committed)
            if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
                raise RequestValidationError("offset must be a non-negative integer")
            if log is None:
                records: list[Record] = []
                start_offset = 0
                next_offset = 0
            else:
                if log.status == PartitionStatus.DAMAGED:
                    raise BrokerOperationError("PARTITION_DAMAGED", "Partition is damaged")
                records = await asyncio.to_thread(log.read, offset, max_records)
                start_offset = log.start_offset
                next_offset = log.next_offset
            checkpoint = (
                self._checkpoints.get((group_id, topic, partition_id)) if group_id else None
            )
            if committed > next_offset:
                raise BrokerOperationError(
                    "COMMITTED_OFFSET_OUT_OF_RANGE",
                    f"Committed offset {committed} exceeds partition end {next_offset}",
                )

        return self._response(
            request,
            MessageType.FETCH_RESPONSE,
            {
                "topic": topic,
                "partition_id": partition_id,
                "records": [self._record_to_payload(record) for record in records],
                "start_offset": start_offset,
                "next_offset": next_offset,
                "committed_offset": committed,
                "checkpoint_offset": checkpoint[0] if checkpoint else None,
                "checkpoint_state": (
                    base64.b64encode(checkpoint[1]).decode("ascii") if checkpoint else None
                ),
            },
        )

    async def _commit_offset(self, request: Envelope) -> Envelope:
        payload = request.payload
        group_id = self._required_string(payload, "group_id")
        topic = self._required_string(payload, "topic")
        self._validate_name(group_id, "group_id")
        self._validate_name(topic, "topic")
        partition_id = self._required_int(payload, "partition_id", minimum=0)
        partition_count = await self._topic_partition_count(topic)
        if partition_id >= partition_count:
            raise RequestValidationError("partition_id is outside the topic partition range")
        await self._validate_partition_leader(topic, partition_id)
        await self._authorize_group_request(request, group_id, topic, partition_id, "COMMIT_OFFSET")
        next_offset = self._required_int(payload, "next_offset", minimum=0)
        checkpoint_state = self._parse_checkpoint_state(payload.get("checkpoint_state"))

        key = (topic, partition_id)
        partition_lock = self._partition_locks.setdefault(key, asyncio.Lock())
        async with partition_lock:
            log = await self._get_log(key, create=False)
            end_offset = log.next_offset if log is not None else 0
            if log is not None and log.status == PartitionStatus.DAMAGED:
                raise BrokerOperationError("PARTITION_DAMAGED", "Partition is damaged")
            if next_offset > end_offset:
                raise RequestValidationError(
                    f"next_offset {next_offset} exceeds partition end offset {end_offset}"
                )
            offset_key = (group_id, topic, partition_id)
            async with self._offset_lock:
                previous = self._committed_offsets.get(offset_key, 0)
                previous_checkpoint = self._checkpoints.get(offset_key)
                if next_offset < previous:
                    raise RequestValidationError(
                        f"next_offset {next_offset} is less than committed offset {previous}"
                    )
                self._committed_offsets[offset_key] = next_offset
                if checkpoint_state is None:
                    self._checkpoints.pop(offset_key, None)
                else:
                    self._checkpoints[offset_key] = (next_offset, checkpoint_state)
                try:
                    await asyncio.to_thread(self._save_committed_offsets)
                except OSError:
                    if previous:
                        self._committed_offsets[offset_key] = previous
                    else:
                        self._committed_offsets.pop(offset_key, None)
                    if previous_checkpoint is not None:
                        self._checkpoints[offset_key] = previous_checkpoint
                    else:
                        self._checkpoints.pop(offset_key, None)
                    raise

        return self._response(
            request,
            MessageType.COMMIT_ACK,
            {
                "group_id": group_id,
                "topic": topic,
                "partition_id": partition_id,
                "next_offset": next_offset,
                "checkpoint_offset": next_offset if checkpoint_state is not None else None,
            },
        )

    async def _get_log(self, key: tuple[str, int], *, create: bool) -> PartitionLog | None:
        if key in self._damaged_partitions:
            raise BrokerOperationError("PARTITION_DAMAGED", "Partition is damaged")
        log = self._logs.get(key)
        if log is not None or not create:
            return log

        log = PartitionLog(topic=key[0], partition_id=key[1], config=self._config)
        await asyncio.to_thread(log.initialize)
        self._logs[key] = log
        return log

    def _load_committed_offsets(self) -> None:
        path = self._offsets_path
        if not path.exists():
            self._committed_offsets.clear()
            self._checkpoints.clear()
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"Cannot load committed offsets from {path}") from exc
        if not isinstance(data, dict):
            raise RuntimeError(f"Invalid committed-offset metadata in {path}")
        if data.get("version") == 2:
            offset_data = data.get("offsets")
            checkpoint_data = data.get("checkpoints")
            if not isinstance(offset_data, dict) or not isinstance(checkpoint_data, dict):
                raise RuntimeError(f"Invalid committed-offset metadata in {path}")
        else:
            offset_data = data
            checkpoint_data = {}

        offsets: dict[tuple[str, str, int], int] = {}
        for group_id, topics in offset_data.items():
            self._validate_name(group_id, "group_id")
            if not isinstance(topics, dict):
                raise RuntimeError(f"Invalid committed-offset metadata for group {group_id!r}")
            for topic, partitions in topics.items():
                self._validate_name(topic, "topic")
                if not isinstance(partitions, dict):
                    raise RuntimeError(f"Invalid committed-offset metadata for topic {topic!r}")
                for partition_text, offset in partitions.items():
                    try:
                        partition_id = int(partition_text)
                    except (TypeError, ValueError) as exc:
                        raise RuntimeError(
                            "Invalid partition id in committed-offset metadata"
                        ) from exc
                    if (
                        partition_id < 0
                        or partition_id >= self._partition_count(topic)
                        or isinstance(offset, bool)
                        or not isinstance(offset, int)
                        or offset < 0
                    ):
                        raise RuntimeError("Invalid value in committed-offset metadata")
                    offsets[(group_id, topic, partition_id)] = offset
        self._committed_offsets = offsets
        checkpoints: dict[tuple[str, str, int], tuple[int, bytes]] = {}
        for group_id, topics in checkpoint_data.items():
            self._validate_name(group_id, "group_id")
            if not isinstance(topics, dict):
                raise RuntimeError("Invalid checkpoint metadata")
            for topic, partitions in topics.items():
                self._validate_name(topic, "topic")
                if not isinstance(partitions, dict):
                    raise RuntimeError("Invalid checkpoint metadata")
                for partition_text, checkpoint in partitions.items():
                    try:
                        partition_id = int(partition_text)
                    except (TypeError, ValueError) as exc:
                        raise RuntimeError("Invalid partition id in checkpoint metadata") from exc
                    if (
                        partition_id < 0
                        or partition_id >= self._partition_count(topic)
                        or not isinstance(checkpoint, dict)
                    ):
                        raise RuntimeError("Invalid checkpoint metadata")
                    offset = checkpoint.get("offset")
                    state = checkpoint.get("state")
                    if (
                        isinstance(offset, bool)
                        or not isinstance(offset, int)
                        or offset < 0
                        or offset > offsets.get((group_id, topic, partition_id), -1)
                        or not isinstance(state, str)
                    ):
                        raise RuntimeError("Invalid checkpoint metadata")
                    try:
                        state_bytes = base64.b64decode(state, validate=True)
                    except (binascii.Error, ValueError) as exc:
                        raise RuntimeError("Invalid checkpoint state encoding") from exc
                    if len(state_bytes) > MAX_CHECKPOINT_BYTES:
                        raise RuntimeError("Checkpoint state exceeds the configured limit")
                    checkpoints[(group_id, topic, partition_id)] = (offset, state_bytes)
        self._checkpoints = checkpoints

    def _save_committed_offsets(self) -> None:
        offset_data: dict[str, dict[str, dict[str, int]]] = {}
        for (group_id, topic, partition_id), offset in self._committed_offsets.items():
            offset_data.setdefault(group_id, {}).setdefault(topic, {})[str(partition_id)] = offset
        checkpoint_data: dict[str, dict[str, dict[str, dict[str, Any]]]] = {}
        for (group_id, topic, partition_id), (offset, state) in self._checkpoints.items():
            checkpoint_data.setdefault(group_id, {}).setdefault(topic, {})[str(partition_id)] = {
                "offset": offset,
                "state": base64.b64encode(state).decode("ascii"),
            }
        data = {"version": 2, "offsets": offset_data, "checkpoints": checkpoint_data}
        path = self._offsets_path
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = path.with_suffix(".tmp")
        with temporary_path.open("w", encoding="utf-8") as file:
            json.dump(data, file, separators=(",", ":"), sort_keys=True)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary_path, path)

    @staticmethod
    def _parse_checkpoint_state(value: Any) -> bytes | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise RequestValidationError("checkpoint_state must be base64 text")
        try:
            state = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise RequestValidationError("checkpoint_state must be valid base64") from exc
        if len(state) > MAX_CHECKPOINT_BYTES:
            raise RequestValidationError(
                f"checkpoint_state must not exceed {MAX_CHECKPOINT_BYTES} bytes"
            )
        return state

    def _overloaded_response(self, request: Envelope) -> Envelope:
        return self._response(
            request,
            MessageType.OVERLOADED,
            {
                "reason": "Broker connection capacity is exhausted",
                "retry_after_ms": 100,
                "accepted": False,
                "queue_depth": self._active_connections,
                "queue_capacity": self._max_connections,
            },
        )

    @property
    def _offsets_path(self) -> Path:
        return self._config.metadata_dir / "committed-offsets.json"

    def _partition_count(self, topic: str) -> int:
        return self._topic_partitions.get(topic, self._default_partition_count)

    async def _topic_partition_count(self, topic: str) -> int:
        if self._coordinator is None:
            return self._partition_count(topic)
        partitions = await self._coordinator.topic(topic)
        if not partitions:
            raise RequestValidationError(f"Unknown topic {topic!r}")
        count = partitions[0].get("partition_count")
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            raise RequestValidationError("Coordinator returned an invalid partition count")
        return count

    async def _validate_partition_leader(self, topic: str, partition_id: int) -> None:
        if self._coordinator is None:
            return
        route = await self._coordinator.partition_by_id(topic, partition_id)
        broker_id = route.get("broker_id")
        if not isinstance(broker_id, str):
            raise RequestValidationError("Coordinator returned an invalid broker route")
        if broker_id != self._config.broker_id:
            raise BrokerOperationError(
                "NOT_LEADER",
                f"Partition is owned by broker {broker_id!r}",
                retryable=True,
            )

    async def _authorize_group_request(
        self,
        request: Envelope,
        group_id: str,
        topic: str,
        partition_id: int,
        operation: str,
    ) -> None:
        if self._coordinator is None:
            return
        epoch = self._required_int(request.payload, "assignment_epoch", minimum=1)
        await self._coordinator.validate_assignment(
            group_id,
            topic,
            partition_id,
            request.sender_id,
            epoch,
            operation,
        )

    async def _heartbeat_loop(self) -> None:
        assert self._coordinator is not None
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                await self._coordinator.heartbeat(self._config.broker_id)
                now = time.monotonic()
                interval_ms = max(0, int((now - self._last_metrics_time) * 1000))
                self._last_metrics_time = now
                process_metrics = self._metrics_collector.sample()
                p95_latency = (
                    quantiles(self._request_latency_ms, n=20)[18]
                    if len(self._request_latency_ms) >= 2
                    else (self._request_latency_ms[0] if self._request_latency_ms else 0.0)
                )
                processing_rate = self._requests_since_metrics / max(interval_ms / 1000, 0.001)
                self._requests_since_metrics = 0
                self._request_latency_ms.clear()
                await self._coordinator.report_metrics(
                    self._instance_id,
                    interval_ms,
                    {
                        "cpu_percent": process_metrics.cpu_percent,
                        "memory_percent": process_metrics.memory_percent,
                        "queue_depth": self._active_connections,
                        "queue_capacity": self._max_connections,
                        "p95_latency_ms": p95_latency,
                        "processing_rate": processing_rate,
                        "in_flight": self._active_connections,
                    },
                )
            except (BrokerRequestError, OSError, TimeoutError, ProtocolError) as exc:
                logger.warning(
                    "Broker heartbeat failed: broker_id=%s error=%s",
                    self._config.broker_id,
                    exc,
                )

    @staticmethod
    def _validate_name(value: str, field: str) -> None:
        if not _NAME_PATTERN.fullmatch(value):
            raise RequestValidationError(
                f"{field} must start with an alphanumeric character and contain "
                "only letters, digits, '.', '_' or '-' (max 128 characters)"
            )

    @staticmethod
    def _required_string(payload: dict[str, Any], name: str) -> str:
        value = payload.get(name)
        if not isinstance(value, str) or not value:
            raise RequestValidationError(f"{name} must be a non-empty string")
        return value

    @staticmethod
    def _required_int(
        payload: dict[str, Any], name: str, *, minimum: int, default: int | None = None
    ) -> int:
        value = payload.get(name, default)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise RequestValidationError(
                f"{name} must be an integer greater than or equal to {minimum}"
            )
        return value

    @staticmethod
    def _parse_event(topic: str, data: dict[str, Any]) -> Event:
        event_id = data.get("event_id")
        partition_key = data.get("partition_key")
        timestamp_ms = data.get("timestamp_ms")
        schema_version = data.get("schema_version")
        payload = data.get("payload")
        headers = data.get("headers", {})
        if not isinstance(event_id, str) or not event_id:
            raise RequestValidationError("event.event_id must be a non-empty string")
        if not isinstance(partition_key, str):
            raise RequestValidationError("event.partition_key must be a string")
        if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, int):
            raise RequestValidationError("event.timestamp_ms must be an integer")
        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or not 0 <= schema_version <= 0xFFFF
        ):
            raise RequestValidationError("event.schema_version must be between 0 and 65535")
        if not isinstance(payload, str):
            raise RequestValidationError("event.payload must be base64 text")
        try:
            payload_bytes = base64.b64decode(payload, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise RequestValidationError("event.payload must be valid base64") from exc
        if not isinstance(headers, dict) or any(
            not isinstance(key, str) or not isinstance(value, str) for key, value in headers.items()
        ):
            raise RequestValidationError("event.headers must contain string keys and values")
        return Event(
            event_id=event_id,
            topic=topic,
            partition_key=partition_key,
            timestamp_ms=timestamp_ms,
            schema_version=schema_version,
            payload=payload_bytes,
            headers=headers,
        )

    @staticmethod
    def _record_to_payload(record: Record) -> dict[str, Any]:
        return {
            "offset": record.offset,
            "event_id": record.event_id,
            "timestamp_ms": record.timestamp_ms,
            "schema_version": record.schema_version,
            "partition_key": record.partition_key,
            "headers": record.headers,
            "payload": base64.b64encode(record.payload).decode("ascii"),
        }

    def _response(
        self, request: Envelope, message_type: MessageType, payload: dict[str, Any]
    ) -> Envelope:
        return Envelope(
            message_type=message_type,
            request_id=request.request_id,
            sender_id=self._config.broker_id,
            deadline_ms=0,
            payload=payload,
        )

    def _error_response(
        self,
        request: Envelope,
        code: str,
        message: str,
        *,
        retryable: bool = False,
    ) -> Envelope:
        return self._response(
            request,
            MessageType.ERROR,
            {"code": code, "message": message, "retryable": retryable},
        )


async def _run_server(args: argparse.Namespace) -> None:
    config = BrokerConfig(
        broker_id=args.broker_id,
        data_dir=Path(args.data_dir),
        host=args.host,
        port=args.port,
    )
    server = BrokerServer(
        config,
        default_partition_count=args.partitions,
        coordinator_host=args.coordinator_host,
        coordinator_port=args.coordinator_port,
    )
    await server.start()
    try:
        await server.serve_forever()
    finally:
        await server.close()


def main() -> None:
    """Run a development broker process."""
    parser = argparse.ArgumentParser(description="Run a MeshFlow broker")
    parser.add_argument("--broker-id", default="broker-1")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9500)
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--coordinator-host")
    parser.add_argument("--coordinator-port", type=int, default=9501)
    asyncio.run(_run_server(parser.parse_args()))


if __name__ == "__main__":
    main()
