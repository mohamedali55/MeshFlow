"""Bounded JSON message framing for MeshFlow TCP requests."""

from __future__ import annotations

import asyncio
import json
import math
import struct
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import uuid4

PROTOCOL_VERSION = 1
FRAME_PREFIX_SIZE = 4
DEFAULT_MAX_FRAME_BYTES = 4 * 1024 * 1024
MAX_REQUEST_ID_LENGTH = 64
MAX_SENDER_ID_LENGTH = 128


class MessageType(StrEnum):
    PUBLISH = "PUBLISH"
    PUBLISH_ACK = "PUBLISH_ACK"
    FETCH = "FETCH"
    FETCH_RESPONSE = "FETCH_RESPONSE"
    COMMIT_OFFSET = "COMMIT_OFFSET"
    COMMIT_ACK = "COMMIT_ACK"
    REGISTER_NODE = "REGISTER_NODE"
    HEARTBEAT = "HEARTBEAT"
    CREATE_TOPIC = "CREATE_TOPIC"
    GET_TOPIC = "GET_TOPIC"
    GET_PARTITION = "GET_PARTITION"
    JOIN_GROUP = "JOIN_GROUP"
    GET_ASSIGNMENTS = "GET_ASSIGNMENTS"
    ACK_REVOKE = "ACK_REVOKE"
    LEAVE_GROUP = "LEAVE_GROUP"
    VALIDATE_ASSIGNMENT = "VALIDATE_ASSIGNMENT"
    ASSIGNMENT_VALIDATED = "ASSIGNMENT_VALIDATED"
    OPTIMIZE_GROUP = "OPTIMIZE_GROUP"
    GET_DECISIONS = "GET_DECISIONS"
    GET_METRICS = "GET_METRICS"
    GET_CLUSTER_STATUS = "GET_CLUSTER_STATUS"
    NODE_STATUS = "NODE_STATUS"
    ASSIGN_PARTITIONS = "ASSIGN_PARTITIONS"
    REVOKE_PARTITIONS = "REVOKE_PARTITIONS"
    METRICS_REPORT = "METRICS_REPORT"
    METRICS_REQUEST = "METRICS_REQUEST"
    CONFIG_UPDATE = "CONFIG_UPDATE"
    DRAIN_NODE = "DRAIN_NODE"
    SHUTDOWN = "SHUTDOWN"
    ERROR = "ERROR"
    NOT_LEADER = "NOT_LEADER"
    STALE_EPOCH = "STALE_EPOCH"
    OVERLOADED = "OVERLOADED"


class ProtocolError(Exception):
    """Raised when a peer sends an invalid or unsupported protocol frame."""


@dataclass(frozen=True, slots=True)
class Envelope:
    """A single versioned protocol message."""

    message_type: MessageType
    request_id: str
    sender_id: str
    deadline_ms: int
    payload: dict[str, Any]
    version: int = PROTOCOL_VERSION

    def to_bytes(self, max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES) -> bytes:
        if isinstance(self.version, bool) or self.version != PROTOCOL_VERSION:
            raise ProtocolError(f"Unsupported protocol version: {self.version!r}")
        if not isinstance(self.message_type, MessageType):
            raise ProtocolError("message_type must be a MessageType")
        if (
            not isinstance(self.request_id, str)
            or not 1 <= len(self.request_id) <= MAX_REQUEST_ID_LENGTH
        ):
            raise ProtocolError("request_id must be a non-empty string of at most 64 characters")
        if (
            not isinstance(self.sender_id, str)
            or not 1 <= len(self.sender_id) <= MAX_SENDER_ID_LENGTH
        ):
            raise ProtocolError("sender_id must be a non-empty string of at most 128 characters")
        if (
            isinstance(self.deadline_ms, bool)
            or not isinstance(self.deadline_ms, int)
            or self.deadline_ms < 0
        ):
            raise ProtocolError("deadline_ms must be a non-negative integer")
        if not isinstance(self.payload, dict):
            raise ProtocolError("payload must be a JSON object")
        body = json.dumps(
            {
                "version": self.version,
                "type": self.message_type.value,
                "request_id": self.request_id,
                "sender_id": self.sender_id,
                "deadline_ms": self.deadline_ms,
                "payload": self.payload,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        if not body or len(body) > max_frame_bytes:
            raise ProtocolError(
                f"Frame size {len(body)} is outside allowed range 1..{max_frame_bytes}"
            )
        return struct.pack(">I", len(body)) + body

    @classmethod
    def from_bytes(cls, body: bytes) -> Envelope:
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProtocolError("Frame body must be valid UTF-8 JSON") from exc
        if not isinstance(value, dict):
            raise ProtocolError("Frame body must be a JSON object")

        version = value.get("version")
        if isinstance(version, bool) or version != PROTOCOL_VERSION:
            raise ProtocolError(f"Unsupported protocol version: {version!r}")

        try:
            message_type = MessageType(value.get("type"))
        except (TypeError, ValueError) as exc:
            raise ProtocolError(f"Unsupported message type: {value.get('type')!r}") from exc

        request_id = value.get("request_id")
        sender_id = value.get("sender_id")
        deadline_ms = value.get("deadline_ms")
        payload = value.get("payload")
        if not isinstance(request_id, str) or not 1 <= len(request_id) <= MAX_REQUEST_ID_LENGTH:
            raise ProtocolError("request_id must be a non-empty string of at most 64 characters")
        if not isinstance(sender_id, str) or not 1 <= len(sender_id) <= MAX_SENDER_ID_LENGTH:
            raise ProtocolError("sender_id must be a non-empty string of at most 128 characters")
        if isinstance(deadline_ms, bool) or not isinstance(deadline_ms, int) or deadline_ms < 0:
            raise ProtocolError("deadline_ms must be a non-negative integer")
        if not isinstance(payload, dict):
            raise ProtocolError("payload must be a JSON object")
        return cls(
            message_type=message_type,
            request_id=request_id,
            sender_id=sender_id,
            deadline_ms=deadline_ms,
            payload=payload,
            version=version,
        )

    @classmethod
    def request(
        cls,
        message_type: MessageType,
        sender_id: str,
        payload: dict[str, Any],
        *,
        timeout: float,
        request_id: str | None = None,
    ) -> Envelope:
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be positive")
        return cls(
            message_type=message_type,
            request_id=request_id or str(uuid4()),
            sender_id=sender_id,
            deadline_ms=int((time.time() + timeout) * 1000),
            payload=payload,
        )


async def read_envelope(
    reader: asyncio.StreamReader,
    *,
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    timeout: float = 10.0,
) -> Envelope:
    """Read one bounded frame without trusting its claimed length."""
    try:
        prefix = await asyncio.wait_for(reader.readexactly(FRAME_PREFIX_SIZE), timeout)
    except asyncio.IncompleteReadError as exc:
        raise ProtocolError("Connection closed before frame length was received") from exc
    except TimeoutError as exc:
        raise ProtocolError("Timed out reading frame length") from exc

    (frame_length,) = struct.unpack(">I", prefix)
    if frame_length == 0 or frame_length > max_frame_bytes:
        raise ProtocolError(
            f"Frame length {frame_length} is outside allowed range 1..{max_frame_bytes}"
        )
    try:
        body = await asyncio.wait_for(reader.readexactly(frame_length), timeout)
    except asyncio.IncompleteReadError as exc:
        raise ProtocolError(
            f"Connection closed during frame body: {len(exc.partial)} of {frame_length} bytes"
        ) from exc
    except TimeoutError as exc:
        raise ProtocolError("Timed out reading frame body") from exc
    return Envelope.from_bytes(body)


async def write_envelope(
    writer: asyncio.StreamWriter,
    envelope: Envelope,
    *,
    max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    timeout: float = 10.0,
) -> None:
    """Write one bounded frame and wait for the stream buffer to drain."""
    writer.write(envelope.to_bytes(max_frame_bytes))
    try:
        await asyncio.wait_for(writer.drain(), timeout)
    except TimeoutError as exc:
        raise ProtocolError("Timed out writing frame") from exc
