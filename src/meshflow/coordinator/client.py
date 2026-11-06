"""Client helpers for coordinator metadata operations."""

from __future__ import annotations

from typing import Any

from meshflow.protocol.client import BrokerClient
from meshflow.protocol.messages import Envelope, MessageType, ProtocolError


class CoordinatorClient(BrokerClient):
    """Typed convenience methods for coordinator requests."""

    async def register_node(
        self,
        node_id: str,
        role: str,
        host: str,
        port: int,
        *,
        capacity: float = 1.0,
    ) -> dict[str, Any]:
        response = await self._request(
            MessageType.REGISTER_NODE,
            {
                "node_id": node_id,
                "role": role,
                "host": host,
                "port": port,
                "capacity": capacity,
            },
        )
        return self._expect(response, MessageType.NODE_STATUS)

    async def create_topic(
        self, topic: str, partition_count: int, *, replication_factor: int = 1
    ) -> list[dict[str, Any]]:
        response = await self._request(
            MessageType.CREATE_TOPIC,
            {
                "topic": topic,
                "partition_count": partition_count,
                "replication_factor": replication_factor,
            },
        )
        return self._expect(response, MessageType.GET_TOPIC)["partitions"]

    async def topic(self, topic: str) -> list[dict[str, Any]]:
        response = await self._request(MessageType.GET_TOPIC, {"topic": topic})
        return self._expect(response, MessageType.GET_TOPIC)["partitions"]

    async def partition_route(self, topic: str, partition_key: str) -> dict[str, Any]:
        response = await self._request(
            MessageType.GET_PARTITION,
            {"topic": topic, "partition_key": partition_key},
        )
        return self._expect(response, MessageType.GET_PARTITION)

    async def partition_by_id(self, topic: str, partition_id: int) -> dict[str, Any]:
        response = await self._request(
            MessageType.GET_PARTITION,
            {"topic": topic, "partition_key": "", "partition_id": partition_id},
        )
        return self._expect(response, MessageType.GET_PARTITION)

    async def join_group(
        self,
        group_id: str,
        worker_id: str,
        topics: list[str],
        *,
        capacity: float = 1.0,
    ) -> list[dict[str, Any]]:
        response = await self._request(
            MessageType.JOIN_GROUP,
            {
                "group_id": group_id,
                "worker_id": worker_id,
                "topics": topics,
                "capacity": capacity,
            },
        )
        return self._expect(response, MessageType.GET_ASSIGNMENTS)["assignments"]

    async def get_assignments(
        self, group_id: str, worker_id: str | None = None
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"group_id": group_id}
        if worker_id is not None:
            payload["worker_id"] = worker_id
        response = await self._request(MessageType.GET_ASSIGNMENTS, payload)
        return self._expect(response, MessageType.GET_ASSIGNMENTS)["assignments"]

    async def acknowledge_revocation(
        self, group_id: str, worker_id: str, topic: str, partition_id: int, epoch: int
    ) -> list[dict[str, Any]]:
        response = await self._request(
            MessageType.ACK_REVOKE,
            {
                "group_id": group_id,
                "worker_id": worker_id,
                "topic": topic,
                "partition_id": partition_id,
                "epoch": epoch,
            },
        )
        return self._expect(response, MessageType.GET_ASSIGNMENTS)["assignments"]

    async def leave_group(self, group_id: str, worker_id: str) -> None:
        response = await self._request(
            MessageType.LEAVE_GROUP, {"group_id": group_id, "worker_id": worker_id}
        )
        self._expect(response, MessageType.NODE_STATUS)

    async def validate_assignment(
        self,
        group_id: str,
        topic: str,
        partition_id: int,
        worker_id: str,
        epoch: int,
        operation: str,
    ) -> None:
        response = await self._request(
            MessageType.VALIDATE_ASSIGNMENT,
            {
                "group_id": group_id,
                "topic": topic,
                "partition_id": partition_id,
                "worker_id": worker_id,
                "epoch": epoch,
                "operation": operation,
            },
        )
        self._expect(response, MessageType.ASSIGNMENT_VALIDATED)

    async def report_metrics(
        self,
        instance_id: str,
        sample_interval_ms: int,
        metrics: dict[str, Any],
    ) -> dict[str, Any]:
        response = await self._request(
            MessageType.METRICS_REPORT,
            {
                "node_id": self._sender_id,
                "instance_id": instance_id,
                "sample_interval_ms": sample_interval_ms,
                **metrics,
            },
        )
        return self._expect(response, MessageType.METRICS_REPORT)

    async def optimize_group(self, group_id: str) -> dict[str, Any]:
        response = await self._request(MessageType.OPTIMIZE_GROUP, {"group_id": group_id})
        return self._expect(response, MessageType.OPTIMIZE_GROUP)

    async def decisions(self, group_id: str, *, limit: int = 100) -> list[dict[str, Any]]:
        response = await self._request(
            MessageType.GET_DECISIONS, {"group_id": group_id, "limit": limit}
        )
        return self._expect(response, MessageType.GET_DECISIONS)["decisions"]

    async def metrics(self, node_id: str | None = None) -> list[dict[str, Any]]:
        payload = {} if node_id is None else {"node_id": node_id}
        response = await self._request(MessageType.GET_METRICS, payload)
        return self._expect(response, MessageType.GET_METRICS)["metrics"]

    async def cluster_status(self) -> dict[str, Any]:
        """Fetch a read-only coordinator snapshot for operational inspection."""
        response = await self._request(MessageType.GET_CLUSTER_STATUS, {})
        return self._expect(response, MessageType.GET_CLUSTER_STATUS)

    async def heartbeat(self, node_id: str) -> dict[str, Any]:
        response = await self._request(MessageType.HEARTBEAT, {"node_id": node_id})
        return self._expect(response, MessageType.NODE_STATUS)

    @staticmethod
    def _expect(response: Envelope, expected: MessageType) -> dict[str, Any]:
        if response.message_type != expected:
            raise ProtocolError(
                f"Expected {expected.value}, received {response.message_type.value}"
            )
        return response.payload
