"""Explicit, bounded failure injection for isolated development experiments."""

from __future__ import annotations

import asyncio
import errno
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TypeVar

T = TypeVar("T")


class BoundedDiskWriter:
    """An in-memory writer that raises ENOSPC after a configured byte budget."""

    def __init__(self, capacity_bytes: int) -> None:
        if (
            isinstance(capacity_bytes, bool)
            or not isinstance(capacity_bytes, int)
            or capacity_bytes < 0
        ):
            raise ValueError("capacity_bytes must be a non-negative integer")
        self.capacity_bytes = capacity_bytes
        self._data = bytearray()

    @property
    def data(self) -> bytes:
        return bytes(self._data)

    def write(self, data: bytes) -> int:
        if not isinstance(data, bytes):
            raise TypeError("data must be bytes")
        if len(self._data) + len(data) > self.capacity_bytes:
            raise OSError(errno.ENOSPC, "simulated disk is full")
        self._data.extend(data)
        return len(data)


class FailureInjector:
    """Failure helpers restricted to caller-owned paths and process handles."""

    @staticmethod
    def corrupt_log(path: Path, *, root: Path, offset: int) -> None:
        """Flip one byte in a non-empty log file beneath the specified test root."""
        resolved_root = root.resolve()
        resolved_path = path.resolve()
        try:
            resolved_path.relative_to(resolved_root)
        except ValueError as exc:
            raise ValueError("failure target must be inside the supplied root") from exc
        data = bytearray(resolved_path.read_bytes())
        if isinstance(offset, bool) or not isinstance(offset, int) or not 0 <= offset < len(data):
            raise ValueError("offset must identify a byte in the target file")
        data[offset] ^= 0x01
        resolved_path.write_bytes(data)

    @staticmethod
    async def with_network_delay(
        delay_seconds: float,
        operation: Callable[..., Awaitable[T]],
        *args: object,
        **kwargs: object,
    ) -> T:
        """Delay one explicitly supplied async operation."""
        if delay_seconds < 0:
            raise ValueError("delay_seconds must not be negative")
        await asyncio.sleep(delay_seconds)
        return await operation(*args, **kwargs)

    @staticmethod
    async def terminate_process(process: asyncio.subprocess.Process) -> int:
        """Terminate and wait for an explicitly supplied child process handle."""
        if process.returncode is None:
            process.terminate()
        return await process.wait()
