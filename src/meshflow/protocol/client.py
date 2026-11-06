"""Shared asynchronous TCP request client with bounded retry backoff."""

from __future__ import annotations

import asyncio
import math
import random
from typing import Any
from uuid import uuid4

from meshflow.protocol.messages import (
    DEFAULT_MAX_FRAME_BYTES,
    Envelope,
    MessageType,
    ProtocolError,
    read_envelope,
    write_envelope,
)


class BrokerRequestError(Exception):
    """A structured error returned by a broker."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        retry_after_ms: int = 0,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.retry_after_ms = retry_after_ms


class BrokerClient:
    """Base client that makes one request per TCP connection."""

    def __init__(
        self,
        host: str,
        port: int,
        *,
        sender_id: str,
        timeout: float = 5.0,
        max_retries: int = 3,
        backoff_base: float = 0.05,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
    ) -> None:
        if not host:
            raise ValueError("host must not be empty")
        if not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        if not sender_id or len(sender_id) > 128:
            raise ValueError("sender_id must contain 1 to 128 characters")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be positive")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries must not be negative")
        if not math.isfinite(backoff_base) or backoff_base < 0:
            raise ValueError("backoff_base must not be negative")
        if max_frame_bytes <= 0:
            raise ValueError("max_frame_bytes must be positive")
        self._host = host
        self._port = port
        self._sender_id = sender_id
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._max_frame_bytes = max_frame_bytes

    async def _request(self, message_type: MessageType, payload: dict[str, Any]) -> Envelope:
        request_id = str(uuid4())
        last_error: Exception | None = None

        for attempt in range(self._max_retries + 1):
            request = Envelope.request(
                message_type,
                self._sender_id,
                payload,
                timeout=self._timeout,
                request_id=request_id,
            )
            try:
                response = await self._request_once(request)
            except (OSError, TimeoutError, ProtocolError) as exc:
                last_error = exc
                if attempt >= self._max_retries:
                    break
                await self._backoff(attempt)
                continue

            if response.request_id != request_id:
                raise ProtocolError(
                    f"Response request_id {response.request_id!r} does not match "
                    f"request {request_id!r}"
                )
            if response.message_type == MessageType.OVERLOADED:
                reason = response.payload.get("reason")
                retry_after_ms = response.payload.get("retry_after_ms")
                accepted = response.payload.get("accepted")
                if (
                    not isinstance(reason, str)
                    or isinstance(retry_after_ms, bool)
                    or not isinstance(retry_after_ms, int)
                    or retry_after_ms < 0
                    or accepted is not False
                ):
                    raise ProtocolError("Broker returned a malformed OVERLOADED response")
                error = BrokerRequestError(
                    "OVERLOADED", reason, retryable=True, retry_after_ms=retry_after_ms
                )
                if attempt < self._max_retries:
                    last_error = error
                    await self._backoff(attempt, minimum_delay=retry_after_ms / 1000)
                    continue
                raise error
            if response.message_type == MessageType.ERROR:
                code = response.payload.get("code")
                message = response.payload.get("message")
                retryable = response.payload.get("retryable", False)
                if (
                    not isinstance(code, str)
                    or not isinstance(message, str)
                    or not isinstance(retryable, bool)
                ):
                    raise ProtocolError("Broker returned a malformed ERROR response")
                error = BrokerRequestError(code, message, retryable=retryable)
                if retryable and attempt < self._max_retries:
                    last_error = error
                    await self._backoff(attempt, minimum_delay=error.retry_after_ms / 1000)
                    continue
                raise error
            return response

        raise ConnectionError(
            f"Broker request failed after {self._max_retries + 1} attempts"
        ) from last_error

    async def request(self, message_type: MessageType, payload: dict[str, Any]) -> Envelope:
        """Send a protocol request to this endpoint."""
        return await self._request(message_type, payload)

    async def _request_once(self, request: Envelope) -> Envelope:
        reader: asyncio.StreamReader
        writer: asyncio.StreamWriter
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port), timeout=self._timeout
        )
        try:
            await write_envelope(
                writer,
                request,
                max_frame_bytes=self._max_frame_bytes,
                timeout=self._timeout,
            )
            return await read_envelope(
                reader,
                max_frame_bytes=self._max_frame_bytes,
                timeout=self._timeout,
            )
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def _backoff(self, attempt: int, *, minimum_delay: float = 0.0) -> None:
        delay = min(self._backoff_base * (2**attempt), 2.0)
        await asyncio.sleep(min(max(random.uniform(0.5, 1.5) * delay, minimum_delay), 2.0))
