"""SQLite-backed coordinator service over the MeshFlow TCP protocol."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sqlite3
import time
from math import isfinite
from pathlib import Path
from typing import Any

from meshflow.coordinator.metadata import (
    AssignmentValidationError,
    MetadataStore,
    PartitionAssignment,
)
from meshflow.coordinator.optimizer import PressureConfig
from meshflow.protocol.messages import (
    Envelope,
    MessageType,
    ProtocolError,
    read_envelope,
    write_envelope,
)

logger = logging.getLogger(__name__)


class CoordinatorServer:
    """Serves metadata, broker registration, topic placement, and group joins."""

    def __init__(
        self,
        store: MetadataStore,
        *,
        host: str = "127.0.0.1",
        port: int = 9501,
        max_frame_bytes: int = 4 * 1024 * 1024,
        request_timeout: float = 10.0,
        max_connections: int = 100,
        heartbeat_timeout: float = 15.0,
        suspect_grace: float = 5.0,
        membership_check_interval: float = 1.0,
        optimizer_config: PressureConfig | None = None,
    ) -> None:
        if not host:
            raise ValueError("host must not be empty")
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        if max_frame_bytes <= 0 or max_connections <= 0:
            raise ValueError("frame size, timeout, and connection limit must be positive")
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(value)
            or value <= 0
            for value in (
                request_timeout,
                heartbeat_timeout,
                suspect_grace,
                membership_check_interval,
            )
        ):
            raise ValueError("timeouts and membership intervals must be finite and positive")
        self._store = store
        self._host = host
        self._port = port
        self._max_frame_bytes = max_frame_bytes
        self._request_timeout = request_timeout
        self._max_connections = max_connections
        self._heartbeat_timeout_ms = max(1, int(heartbeat_timeout * 1000))
        self._suspect_grace_ms = max(1, int(suspect_grace * 1000))
        self._membership_check_interval = membership_check_interval
        self._optimizer_config = optimizer_config or PressureConfig()
        self._server: asyncio.Server | None = None
        self._membership_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._active_connections = 0

    @property
    def address(self) -> tuple[str, int]:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("Coordinator server is not started")
        address = self._server.sockets[0].getsockname()
        return str(address[0]), int(address[1])

    async def start(self) -> None:
        if self._server is not None:
            raise RuntimeError("Coordinator server is already started")
        self._server = await asyncio.start_server(self._handle_connection, self._host, self._port)
        self._membership_task = asyncio.create_task(self._membership_monitor())
        logger.info("Coordinator listening on %s:%d", *self.address)

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def close(self) -> None:
        server = self._server
        self._server = None
        if server is not None:
            server.close()
            await server.wait_closed()
        membership_task = self._membership_task
        self._membership_task = None
        if membership_task is not None:
            membership_task.cancel()
            await asyncio.gather(membership_task, return_exceptions=True)
        tasks = [task for task in self._tasks if task is not asyncio.current_task()]
        if tasks:
            await asyncio.gather(*tasks)

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._tasks.add(task)
        self._active_connections += 1
        try:
            if self._active_connections > self._max_connections:
                return
            try:
                request = await read_envelope(
                    reader,
                    max_frame_bytes=self._max_frame_bytes,
                    timeout=self._request_timeout,
                )
            except (ProtocolError, OSError) as exc:
                logger.warning("Rejected invalid coordinator frame: %s", exc)
                return

            if request.deadline_ms and request.deadline_ms < int(time.time() * 1000):
                response = self._error(request, "DEADLINE_EXCEEDED", "Request deadline has expired")
            else:
                response = await self._dispatch(request)
            try:
                await write_envelope(
                    writer,
                    response,
                    max_frame_bytes=self._max_frame_bytes,
                    timeout=self._request_timeout,
                )
            except (ProtocolError, OSError) as exc:
                logger.warning("Could not send coordinator response: %s", exc)
        finally:
            self._active_connections -= 1
            if task is not None:
                self._tasks.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                logger.debug("Coordinator connection did not close cleanly", exc_info=True)

    async def _dispatch(self, request: Envelope) -> Envelope:
        try:
            message = request.message_type
            payload = request.payload
            if message == MessageType.REGISTER_NODE:
                node_id = self._string(payload, "node_id")
                if node_id != request.sender_id:
                    raise ValueError("node_id must match the request sender_id")
                role = self._string(payload, "role")
                host = self._string(payload, "host")
                port = self._integer(payload, "port", minimum=0)
                capacity = self._number(payload, "capacity", default=1.0)
                node = await asyncio.to_thread(
                    self._store.register_node, node_id, role, host, port, capacity
                )
                return self._response(
                    request,
                    MessageType.NODE_STATUS,
                    {
                        "node_id": node.node_id,
                        "role": node.role,
                        "status": node.status,
                        "host": node.host,
                        "port": node.port,
                    },
                )
            if message == MessageType.HEARTBEAT:
                node_id = self._string(payload, "node_id")
                if node_id != request.sender_id:
                    raise ValueError("node_id must match the request sender_id")
                node = await asyncio.to_thread(self._store.heartbeat, node_id)
                return self._response(
                    request,
                    MessageType.NODE_STATUS,
                    {"node_id": node.node_id, "status": node.status},
                )
            if message == MessageType.VALIDATE_ASSIGNMENT:
                await asyncio.to_thread(
                    self._store.validate_assignment,
                    self._string(payload, "group_id"),
                    self._string(payload, "topic"),
                    self._integer(payload, "partition_id", minimum=0),
                    self._string(payload, "worker_id"),
                    request.sender_id,
                    self._integer(payload, "epoch", minimum=1),
                    self._string(payload, "operation"),
                )
                return self._response(request, MessageType.ASSIGNMENT_VALIDATED, {"valid": True})
            if message == MessageType.METRICS_REPORT:
                node_id = self._string(payload, "node_id")
                if node_id != request.sender_id:
                    raise ValueError("node_id must match the request sender_id")
                instance_id = self._string(payload, "instance_id")
                sample_interval_ms = self._integer(payload, "sample_interval_ms", minimum=0)
                metrics = await asyncio.to_thread(
                    self._store.report_metrics,
                    node_id,
                    instance_id,
                    sample_interval_ms,
                    payload,
                    config=self._optimizer_config,
                )
                group_id = payload.get("group_id")
                decision = None
                if group_id is not None:
                    if not isinstance(group_id, str):
                        raise ValueError("group_id must be a string")
                    decision = await asyncio.to_thread(
                        self._store.optimize_group,
                        group_id,
                        config=self._optimizer_config,
                    )
                return self._response(
                    request,
                    MessageType.METRICS_REPORT,
                    {"metrics": metrics, "decision": decision},
                )
            if message == MessageType.OPTIMIZE_GROUP:
                group_id = self._string(payload, "group_id")
                decision = await asyncio.to_thread(
                    self._store.optimize_group,
                    group_id,
                    config=self._optimizer_config,
                )
                return self._response(request, MessageType.OPTIMIZE_GROUP, decision)
            if message == MessageType.GET_DECISIONS:
                group_id = self._string(payload, "group_id")
                limit = self._integer(payload, "limit", minimum=1)
                decisions = await asyncio.to_thread(
                    self._store.get_decisions, group_id, limit=limit
                )
                return self._response(request, MessageType.GET_DECISIONS, {"decisions": decisions})
            if message == MessageType.GET_METRICS:
                node_id = payload.get("node_id")
                if node_id is not None and not isinstance(node_id, str):
                    raise ValueError("node_id must be a string")
                metrics = await asyncio.to_thread(self._store.get_metrics, node_id)
                return self._response(request, MessageType.GET_METRICS, {"metrics": metrics})
            if message == MessageType.GET_CLUSTER_STATUS:
                status = await asyncio.to_thread(self._store.cluster_status)
                return self._response(request, MessageType.GET_CLUSTER_STATUS, status)
            if message == MessageType.CREATE_TOPIC:
                topic = self._string(payload, "topic")
                count = self._integer(payload, "partition_count", minimum=1)
                replication_factor = (
                    self._integer(payload, "replication_factor", minimum=1)
                    if "replication_factor" in payload
                    else 1
                )
                partitions = await asyncio.to_thread(
                    self._store.create_topic, topic, count, replication_factor
                )
                return self._response(
                    request, MessageType.GET_TOPIC, {"topic": topic, "partitions": partitions}
                )
            if message == MessageType.GET_TOPIC:
                topic = self._string(payload, "topic")
                partitions = await asyncio.to_thread(self._store.get_topic, topic)
                return self._response(
                    request, MessageType.GET_TOPIC, {"topic": topic, "partitions": partitions}
                )
            if message == MessageType.GET_PARTITION:
                topic = self._string(payload, "topic")
                partition_key = self._string(payload, "partition_key", allow_empty=True)
                partitions = await asyncio.to_thread(self._store.get_topic, topic)
                if not partitions:
                    raise KeyError(f"Unknown topic {topic!r}")
                partition_count = int(partitions[0]["partition_count"])
                partition_id = (
                    self._integer(payload, "partition_id", minimum=0)
                    if "partition_id" in payload
                    else None
                )
                if partition_id is None:
                    from meshflow.common.hashing import select_partition

                    partition_id = select_partition(partition_key, partition_count)
                elif partition_id >= partition_count:
                    raise ValueError("partition_id is outside the topic partition range")
                route = await asyncio.to_thread(
                    self._store.get_partition_route, topic, partition_id
                )
                return self._response(
                    request,
                    MessageType.GET_PARTITION,
                    {**route, "partition_count": partition_count},
                )
            if message == MessageType.JOIN_GROUP:
                group_id = self._string(payload, "group_id")
                worker_id = self._string(payload, "worker_id")
                if worker_id != request.sender_id:
                    raise ValueError("worker_id must match the request sender_id")
                topics = payload.get("topics")
                if not isinstance(topics, list):
                    raise ValueError("topics must be a list of topic names")
                capacity = self._number(payload, "capacity", default=1.0)
                assignments = await asyncio.to_thread(
                    self._store.join_group,
                    group_id,
                    worker_id,
                    topics,
                    capacity=capacity,
                )
                return self._response(
                    request,
                    MessageType.GET_ASSIGNMENTS,
                    {
                        "group_id": group_id,
                        "worker_id": worker_id,
                        "assignments": [self._assignment_payload(item) for item in assignments],
                    },
                )
            if message == MessageType.GET_ASSIGNMENTS:
                group_id = self._string(payload, "group_id")
                worker_id = payload.get("worker_id")
                if worker_id is not None and not isinstance(worker_id, str):
                    raise ValueError("worker_id must be a string")
                assignments = await asyncio.to_thread(
                    self._store.get_assignments, group_id, worker_id
                )
                return self._response(
                    request,
                    MessageType.GET_ASSIGNMENTS,
                    {
                        "group_id": group_id,
                        "assignments": [self._assignment_payload(item) for item in assignments],
                    },
                )
            if message == MessageType.ACK_REVOKE:
                group_id = self._string(payload, "group_id")
                worker_id = self._string(payload, "worker_id")
                if worker_id != request.sender_id:
                    raise ValueError("worker_id must match the request sender_id")
                assignments = await asyncio.to_thread(
                    self._store.acknowledge_revocation,
                    group_id,
                    worker_id,
                    self._string(payload, "topic"),
                    self._integer(payload, "partition_id", minimum=0),
                    self._integer(payload, "epoch", minimum=1),
                )
                return self._response(
                    request,
                    MessageType.GET_ASSIGNMENTS,
                    {
                        "group_id": group_id,
                        "worker_id": worker_id,
                        "assignments": [self._assignment_payload(item) for item in assignments],
                    },
                )
            if message == MessageType.LEAVE_GROUP:
                group_id = self._string(payload, "group_id")
                worker_id = self._string(payload, "worker_id")
                if worker_id != request.sender_id:
                    raise ValueError("worker_id must match the request sender_id")
                await asyncio.to_thread(self._store.leave_group, group_id, worker_id)
                return self._response(
                    request, MessageType.NODE_STATUS, {"group_id": group_id, "left": True}
                )
            return self._error(request, "UNSUPPORTED_MESSAGE", f"Unsupported: {message.value}")
        except AssignmentValidationError as exc:
            return self._error(request, exc.code, str(exc))
        except (ValueError, KeyError) as exc:
            return self._error(request, "INVALID_REQUEST", str(exc))
        except (OSError, RuntimeError, sqlite3.Error):
            logger.exception(
                "Coordinator request failed: type=%s request_id=%s",
                request.message_type.value,
                request.request_id,
            )
            return self._error(
                request, "COORDINATOR_ERROR", "Coordinator could not complete request"
            )

    @staticmethod
    def _assignment_payload(assignment: PartitionAssignment) -> dict[str, object]:
        return {
            "group_id": assignment.group_id,
            "topic": assignment.topic,
            "partition_id": assignment.partition_id,
            "worker_id": assignment.worker_id,
            "broker_id": assignment.broker_id,
            "broker_host": assignment.broker_host,
            "broker_port": assignment.broker_port,
            "epoch": assignment.epoch,
            "state": assignment.state,
        }

    async def _membership_monitor(self) -> None:
        while True:
            await asyncio.sleep(self._membership_check_interval)
            try:
                changed = await asyncio.to_thread(
                    self._store.expire_nodes,
                    heartbeat_timeout_ms=self._heartbeat_timeout_ms,
                    suspect_grace_ms=self._suspect_grace_ms,
                )
            except (OSError, RuntimeError, sqlite3.Error):
                logger.exception("Coordinator membership monitor failed")
                continue
            for node in changed:
                logger.warning(
                    "Node membership state changed: node_id=%s status=%s",
                    node.node_id,
                    node.status,
                )

    @staticmethod
    def _string(payload: dict[str, Any], field: str, *, allow_empty: bool = False) -> str:
        value = payload.get(field)
        if not isinstance(value, str) or (not value and not allow_empty):
            kind = "string" if allow_empty else "non-empty string"
            raise ValueError(f"{field} must be a {kind}")
        return value

    @staticmethod
    def _integer(payload: dict[str, Any], field: str, *, minimum: int) -> int:
        value = payload.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ValueError(f"{field} must be an integer greater than or equal to {minimum}")
        return value

    @staticmethod
    def _number(payload: dict[str, Any], field: str, *, default: float) -> float:
        value = payload.get(field, default)
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{field} must be positive")
        return float(value)

    @staticmethod
    def _response(
        request: Envelope, message_type: MessageType, payload: dict[str, Any]
    ) -> Envelope:
        return Envelope(
            message_type=message_type,
            request_id=request.request_id,
            sender_id="coordinator",
            deadline_ms=0,
            payload=payload,
        )

    def _error(self, request: Envelope, code: str, message: str) -> Envelope:
        return self._response(
            request,
            MessageType.ERROR,
            {"code": code, "message": message, "retryable": False},
        )


async def _run(args: argparse.Namespace) -> None:
    optimizer_config = PressureConfig(
        alpha=args.pressure_alpha,
        overloaded_enter=args.overloaded_enter,
        overloaded_exit=args.overloaded_exit,
        minimum_benefit=args.minimum_benefit,
        migration_cost=args.migration_cost,
        cooldown_seconds=args.cooldown_seconds,
        migration_budget=args.migration_budget,
        budget_window_seconds=args.budget_window_seconds,
        max_sample_age_seconds=args.max_sample_age,
    )
    coordinator = CoordinatorServer(
        MetadataStore(args.database),
        host=args.host,
        port=args.port,
        heartbeat_timeout=args.heartbeat_timeout,
        suspect_grace=args.suspect_grace,
        membership_check_interval=args.membership_check_interval,
        optimizer_config=optimizer_config,
    )
    await coordinator.start()
    try:
        await coordinator.serve_forever()
    finally:
        await coordinator.close()


def main() -> None:
    """Run the coordinator process."""
    parser = argparse.ArgumentParser(description="Run a MeshFlow coordinator")
    parser.add_argument("--database", type=Path, default=Path("data/coordinator.sqlite"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9501)
    parser.add_argument("--heartbeat-timeout", type=float, default=15.0)
    parser.add_argument("--suspect-grace", type=float, default=5.0)
    parser.add_argument("--membership-check-interval", type=float, default=1.0)
    parser.add_argument("--pressure-alpha", type=float, default=0.2)
    parser.add_argument("--overloaded-enter", type=float, default=0.85)
    parser.add_argument("--overloaded-exit", type=float, default=0.65)
    parser.add_argument("--minimum-benefit", type=float, default=0.15)
    parser.add_argument("--migration-cost", type=float, default=0.05)
    parser.add_argument("--cooldown-seconds", type=float, default=15.0)
    parser.add_argument("--migration-budget", type=int, default=3)
    parser.add_argument("--budget-window-seconds", type=float, default=60.0)
    parser.add_argument("--max-sample-age", type=float, default=30.0)
    asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    main()
