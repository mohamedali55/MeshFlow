"""Tests for MeshFlow TCP framing and protocol messages."""

from __future__ import annotations

import asyncio
import struct
from uuid import uuid4

import pytest

from meshflow.protocol.client import BrokerClient
from meshflow.protocol.messages import (
    DEFAULT_MAX_FRAME_BYTES,
    Envelope,
    MessageType,
    ProtocolError,
    read_envelope,
)


def _envelope() -> Envelope:
    return Envelope.request(
        MessageType.PUBLISH,
        "test-producer",
        {"topic": "events"},
        timeout=1,
        request_id=str(uuid4()),
    )


def test_envelope_round_trip() -> None:
    original = _envelope()
    frame = original.to_bytes()

    (length,) = struct.unpack(">I", frame[:4])
    restored = Envelope.from_bytes(frame[4:])

    assert length == len(frame) - 4
    assert restored == original


def test_envelope_rejects_oversized_frame() -> None:
    with pytest.raises(ProtocolError, match="outside allowed range"):
        _envelope().to_bytes(max_frame_bytes=10)


def test_envelope_rejects_unknown_message_type() -> None:
    with pytest.raises(ProtocolError, match="Unsupported message type"):
        Envelope.from_bytes(
            b'{"version":1,"type":"UNKNOWN","request_id":"r","sender_id":"s",'
            b'"deadline_ms":1,"payload":{}}'
        )


def test_envelope_rejects_boolean_protocol_version() -> None:
    with pytest.raises(ProtocolError, match="Unsupported protocol version"):
        Envelope.from_bytes(
            b'{"version":true,"type":"PUBLISH","request_id":"r","sender_id":"s",'
            b'"deadline_ms":1,"payload":{}}'
        )


@pytest.mark.asyncio
async def test_read_envelope_rejects_length_before_reading_body() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(struct.pack(">I", DEFAULT_MAX_FRAME_BYTES + 1))

    with pytest.raises(ProtocolError, match="outside allowed range"):
        await read_envelope(reader)


@pytest.mark.asyncio
async def test_read_envelope_rejects_truncated_frame() -> None:
    reader = asyncio.StreamReader()
    reader.feed_data(struct.pack(">I", 10) + b"{}")
    reader.feed_eof()

    with pytest.raises(ProtocolError, match="closed during frame body"):
        await read_envelope(reader)


@pytest.mark.asyncio
async def test_client_retries_explicit_overload_after_server_delay() -> None:
    class OverloadedOnceClient(BrokerClient):
        attempts = 0

        async def _request_once(self, request: Envelope) -> Envelope:
            self.attempts += 1
            if self.attempts == 1:
                return Envelope(
                    message_type=MessageType.OVERLOADED,
                    request_id=request.request_id,
                    sender_id="test-broker",
                    deadline_ms=0,
                    payload={
                        "reason": "capacity exhausted",
                        "retry_after_ms": 0,
                        "accepted": False,
                    },
                )
            return Envelope(
                message_type=MessageType.FETCH_RESPONSE,
                request_id=request.request_id,
                sender_id="test-broker",
                deadline_ms=0,
                payload={},
            )

    client = OverloadedOnceClient(
        "localhost",
        9500,
        sender_id="test-client",
        max_retries=1,
        backoff_base=0,
    )
    response = await client.request(MessageType.FETCH, {"topic": "events"})
    assert response.message_type == MessageType.FETCH_RESPONSE
    assert client.attempts == 2
