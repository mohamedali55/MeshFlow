"""Asynchronous producer client."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Mapping

from meshflow.common.hashing import select_partition
from meshflow.common.types import Event, EventPosition
from meshflow.coordinator.client import CoordinatorClient
from meshflow.protocol.client import BrokerClient, BrokerRequestError
from meshflow.protocol.messages import MessageType, ProtocolError


class Producer(BrokerClient):
    """Publishes events to a broker using deterministic key partitioning."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        sender_id: str = "meshflow-producer",
        partition_counts: Mapping[str, int] | None = None,
        default_partition_count: int = 1,
        timeout: float = 5.0,
        max_retries: int = 3,
        backoff_base: float = 0.05,
        max_frame_bytes: int = 4 * 1024 * 1024,
        max_in_flight: int = 100,
        coordinator_host: str | None = None,
        coordinator_port: int = 9501,
    ) -> None:
        super().__init__(
            host,
            port,
            sender_id=sender_id,
            timeout=timeout,
            max_retries=max_retries,
            backoff_base=backoff_base,
            max_frame_bytes=max_frame_bytes,
        )
        if (
            isinstance(default_partition_count, bool)
            or not isinstance(default_partition_count, int)
            or default_partition_count <= 0
        ):
            raise ValueError("default_partition_count must be positive")
        if (
            isinstance(max_in_flight, bool)
            or not isinstance(max_in_flight, int)
            or max_in_flight <= 0
        ):
            raise ValueError("max_in_flight must be a positive integer")
        self._publish_semaphore = asyncio.Semaphore(max_in_flight)
        self._partition_counts = dict(partition_counts or {})
        if any(
            isinstance(count, bool) or not isinstance(count, int) or count <= 0
            for count in self._partition_counts.values()
        ):
            raise ValueError("topic partition counts must be positive integers")
        self._default_partition_count = default_partition_count
        if coordinator_host is not None and not 0 <= coordinator_port <= 65535:
            raise ValueError("coordinator_port must be between 0 and 65535")
        self._coordinator = (
            CoordinatorClient(
                coordinator_host,
                coordinator_port,
                sender_id=sender_id,
                timeout=timeout,
                max_retries=max_retries,
                backoff_base=backoff_base,
                max_frame_bytes=max_frame_bytes,
            )
            if coordinator_host is not None
            else None
        )

    async def publish(self, event: Event) -> EventPosition:
        """Publish an event and return its broker-assigned position."""
        async with self._publish_semaphore:
            return await self._publish(event)

    async def _publish(self, event: Event) -> EventPosition:
        broker_client: BrokerClient = self
        if self._coordinator is not None:
            route = await self._coordinator.partition_route(event.topic, event.partition_key)
            host = route.get("broker_host")
            port = route.get("broker_port")
            partition_id = route.get("partition_id")
            if (
                not isinstance(host, str)
                or isinstance(port, bool)
                or not isinstance(port, int)
                or isinstance(partition_id, bool)
                or not isinstance(partition_id, int)
            ):
                raise ProtocolError("Coordinator returned an invalid partition route")
            broker_client = BrokerClient(
                host,
                port,
                sender_id=self._sender_id,
                timeout=self._timeout,
                max_retries=self._max_retries,
                backoff_base=self._backoff_base,
                max_frame_bytes=self._max_frame_bytes,
            )
        else:
            partition_count = self._partition_counts.get(event.topic, self._default_partition_count)
            partition_id = select_partition(event.partition_key, partition_count)
        response = await broker_client.request(
            MessageType.PUBLISH,
            {
                "topic": event.topic,
                "partition_id": partition_id,
                "event": {
                    "event_id": event.event_id,
                    "partition_key": event.partition_key,
                    "timestamp_ms": event.timestamp_ms,
                    "schema_version": event.schema_version,
                    "headers": event.headers,
                    "payload": base64.b64encode(event.payload).decode("ascii"),
                },
            },
        )
        if response.message_type != MessageType.PUBLISH_ACK:
            raise ProtocolError(f"Expected PUBLISH_ACK, received {response.message_type.value}")
        payload = response.payload
        topic = payload.get("topic")
        partition_id = payload.get("partition_id")
        offset = payload.get("offset")
        if (
            topic != event.topic
            or isinstance(partition_id, bool)
            or not isinstance(partition_id, int)
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 0
        ):
            raise ProtocolError("Broker returned a malformed PUBLISH_ACK")
        return EventPosition(topic=topic, partition_id=partition_id, offset=offset)


__all__ = ["BrokerRequestError", "Producer"]
