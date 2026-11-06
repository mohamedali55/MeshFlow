"""Asynchronous consumer fetch and offset-commit client."""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass
from typing import Any

from meshflow.broker.serialization import Record
from meshflow.common.types import Event
from meshflow.coordinator.client import CoordinatorClient
from meshflow.protocol.client import BrokerClient, BrokerRequestError
from meshflow.protocol.messages import MessageType, ProtocolError


@dataclass(frozen=True, slots=True)
class FetchResult:
    """Records and offset watermarks returned by a broker."""

    topic: str
    partition_id: int
    records: list[Record]
    start_offset: int
    next_offset: int
    committed_offset: int
    checkpoint_offset: int | None = None
    checkpoint_state: bytes | None = None

    @property
    def events(self) -> list[Event]:
        """Return fetched records as application-facing events."""
        return [record.to_event(self.topic) for record in self.records]


class Consumer(BrokerClient):
    """Fetches events and explicitly commits next-to-read offsets."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        group_id: str,
        sender_id: str | None = None,
        timeout: float = 5.0,
        max_retries: int = 3,
        backoff_base: float = 0.05,
        max_frame_bytes: int = 4 * 1024 * 1024,
        coordinator_host: str | None = None,
        coordinator_port: int = 9501,
    ) -> None:
        if not group_id:
            raise ValueError("group_id must not be empty")
        super().__init__(
            host,
            port,
            sender_id=sender_id or f"consumer-{group_id}",
            timeout=timeout,
            max_retries=max_retries,
            backoff_base=backoff_base,
            max_frame_bytes=max_frame_bytes,
        )
        self.group_id = group_id
        if coordinator_host is not None and not 0 <= coordinator_port <= 65535:
            raise ValueError("coordinator_port must be between 0 and 65535")
        self._coordinator = (
            CoordinatorClient(
                coordinator_host,
                coordinator_port,
                sender_id=sender_id or f"consumer-{group_id}",
                timeout=timeout,
                max_retries=max_retries,
                backoff_base=backoff_base,
                max_frame_bytes=max_frame_bytes,
            )
            if coordinator_host is not None
            else None
        )

    async def _broker_for_partition(self, topic: str, partition_id: int) -> BrokerClient:
        if self._coordinator is None:
            return self
        route = await self._coordinator.partition_by_id(topic, partition_id)
        host = route.get("broker_host")
        port = route.get("broker_port")
        if not isinstance(host, str) or isinstance(port, bool) or not isinstance(port, int):
            raise ProtocolError("Coordinator returned an invalid partition route")
        return BrokerClient(
            host,
            port,
            sender_id=self._sender_id,
            timeout=self._timeout,
            max_retries=self._max_retries,
            backoff_base=self._backoff_base,
            max_frame_bytes=self._max_frame_bytes,
        )

    async def fetch(
        self,
        topic: str,
        partition_id: int,
        *,
        offset: int | None = None,
        max_records: int = 100,
        assignment_epoch: int | None = None,
    ) -> FetchResult:
        """Fetch records, defaulting to this group's committed offset."""
        if assignment_epoch is not None and (
            isinstance(assignment_epoch, bool)
            or not isinstance(assignment_epoch, int)
            or assignment_epoch <= 0
        ):
            raise ValueError("assignment_epoch must be a positive integer")
        payload: dict[str, Any] = {
            "topic": topic,
            "partition_id": partition_id,
            "group_id": self.group_id,
            "max_records": max_records,
        }
        if offset is not None:
            payload["offset"] = offset
        if assignment_epoch is not None:
            payload["assignment_epoch"] = assignment_epoch
        broker = await self._broker_for_partition(topic, partition_id)
        response = await broker.request(MessageType.FETCH, payload)
        if response.message_type != MessageType.FETCH_RESPONSE:
            raise ProtocolError(f"Expected FETCH_RESPONSE, received {response.message_type.value}")
        data = response.payload
        records_data = data.get("records")
        if (
            data.get("topic") != topic
            or isinstance(data.get("partition_id"), bool)
            or data.get("partition_id") != partition_id
            or not isinstance(records_data, list)
        ):
            raise ProtocolError("Broker returned a malformed FETCH_RESPONSE")
        records = [self._parse_record(item) for item in records_data]
        if len(records) > max_records or any(
            right.offset != left.offset + 1 for left, right in zip(records, records[1:])
        ):
            raise ProtocolError("Broker returned a non-contiguous fetch batch")
        start_offset = self._nonnegative_int(data.get("start_offset"), "start_offset")
        next_offset = self._nonnegative_int(data.get("next_offset"), "next_offset")
        committed_offset = self._nonnegative_int(data.get("committed_offset"), "committed_offset")
        checkpoint_offset_raw = data.get("checkpoint_offset")
        checkpoint_state_raw = data.get("checkpoint_state")
        if checkpoint_offset_raw is None and checkpoint_state_raw is None:
            checkpoint_offset = None
            checkpoint_state = None
        elif (
            isinstance(checkpoint_offset_raw, bool)
            or not isinstance(checkpoint_offset_raw, int)
            or checkpoint_offset_raw < 0
            or not isinstance(checkpoint_state_raw, str)
        ):
            raise ProtocolError("Broker returned malformed checkpoint metadata")
        else:
            checkpoint_offset = checkpoint_offset_raw
            try:
                checkpoint_state = base64.b64decode(checkpoint_state_raw, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise ProtocolError("Broker returned invalid checkpoint state") from exc
            if checkpoint_offset > committed_offset:
                raise ProtocolError("Broker returned a checkpoint beyond the committed offset")
        if any(record.offset < start_offset or record.offset >= next_offset for record in records):
            raise ProtocolError("Broker returned a record outside the reported offset range")
        requested_offset = offset if offset is not None else committed_offset
        if records and records[0].offset < requested_offset:
            raise ProtocolError("Broker returned a record before the requested offset")
        return FetchResult(
            topic=topic,
            partition_id=partition_id,
            records=records,
            start_offset=start_offset,
            next_offset=next_offset,
            committed_offset=committed_offset,
            checkpoint_offset=checkpoint_offset,
            checkpoint_state=checkpoint_state,
        )

    async def commit(
        self,
        topic: str,
        partition_id: int,
        next_offset: int,
        *,
        assignment_epoch: int | None = None,
        checkpoint_state: bytes | None = None,
    ) -> int:
        """Persist the next offset and optional application state atomically."""
        if assignment_epoch is not None and (
            isinstance(assignment_epoch, bool)
            or not isinstance(assignment_epoch, int)
            or assignment_epoch <= 0
        ):
            raise ValueError("assignment_epoch must be a positive integer")
        if checkpoint_state is not None:
            if not isinstance(checkpoint_state, bytes):
                raise TypeError("checkpoint_state must be bytes or None")
            if len(checkpoint_state) > 1024 * 1024:
                raise ValueError("checkpoint_state must not exceed 1048576 bytes")
        payload: dict[str, Any] = {
            "group_id": self.group_id,
            "topic": topic,
            "partition_id": partition_id,
            "next_offset": next_offset,
        }
        if checkpoint_state is not None:
            payload["checkpoint_state"] = base64.b64encode(checkpoint_state).decode("ascii")
        if assignment_epoch is not None:
            payload["assignment_epoch"] = assignment_epoch
        broker = await self._broker_for_partition(topic, partition_id)
        response = await broker.request(
            MessageType.COMMIT_OFFSET,
            payload,
        )
        if response.message_type != MessageType.COMMIT_ACK:
            raise ProtocolError(f"Expected COMMIT_ACK, received {response.message_type.value}")
        payload = response.payload
        committed = self._nonnegative_int(payload.get("next_offset"), "next_offset")
        if (
            payload.get("group_id") != self.group_id
            or payload.get("topic") != topic
            or isinstance(payload.get("partition_id"), bool)
            or payload.get("partition_id") != partition_id
            or committed != next_offset
        ):
            raise ProtocolError("Broker returned a malformed COMMIT_ACK")
        expected_checkpoint_offset = next_offset if checkpoint_state is not None else None
        if payload.get("checkpoint_offset") != expected_checkpoint_offset:
            raise ProtocolError("Broker returned a malformed checkpoint acknowledgement")
        return committed

    @staticmethod
    def _parse_record(value: Any) -> Record:
        if not isinstance(value, dict):
            raise ProtocolError("FETCH_RESPONSE records must be JSON objects")
        offset = Consumer._nonnegative_int(value.get("offset"), "record offset")
        timestamp = value.get("timestamp_ms")
        schema_version = value.get("schema_version")
        event_id = value.get("event_id")
        partition_key = value.get("partition_key")
        headers = value.get("headers")
        payload_text = value.get("payload")
        if isinstance(timestamp, bool) or not isinstance(timestamp, int):
            raise ProtocolError("record timestamp_ms must be an integer")
        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or not 0 <= schema_version <= 0xFFFF
        ):
            raise ProtocolError("record schema_version is invalid")
        if not isinstance(event_id, str) or not isinstance(partition_key, str):
            raise ProtocolError("record event_id and partition_key must be strings")
        if not isinstance(headers, dict) or any(
            not isinstance(key, str) or not isinstance(item, str) for key, item in headers.items()
        ):
            raise ProtocolError("record headers must contain string keys and values")
        if not isinstance(payload_text, str):
            raise ProtocolError("record payload must be base64 text")
        try:
            payload = base64.b64decode(payload_text, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ProtocolError("record payload is not valid base64") from exc
        return Record(
            offset=offset,
            timestamp_ms=timestamp,
            event_id=event_id,
            schema_version=schema_version,
            partition_key=partition_key,
            headers=headers,
            payload=payload,
        )

    @staticmethod
    def _nonnegative_int(value: Any, field: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ProtocolError(f"{field} must be a non-negative integer")
        return value


__all__ = ["BrokerRequestError", "Consumer", "FetchResult"]
