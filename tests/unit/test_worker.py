from __future__ import annotations

import pytest

from meshflow.common.types import Event
from meshflow.worker.process import PermanentHandlerError, Worker, _HandlerFailureError


def test_worker_rejects_non_finite_timing_and_capacity_values() -> None:
    with pytest.raises(ValueError):
        Worker("worker", "group", ["events"], capacity=float("nan"))
    with pytest.raises(ValueError):
        Worker("worker", "group", ["events"], retry_backoff_base=float("inf"))
    with pytest.raises(ValueError):
        Worker("worker", "group", ["events"], heartbeat_interval=float("nan"))


@pytest.mark.asyncio
async def test_permanent_handler_error_skips_retries() -> None:
    attempts = 0

    def handler(event: Event) -> None:
        nonlocal attempts
        attempts += 1
        raise PermanentHandlerError(f"invalid event {event.event_id}")

    worker = Worker("worker", "group", ["events"], handler=handler, max_handler_retries=5)
    with pytest.raises(_HandlerFailureError) as failure:
        await worker._process_with_retries(Event.create("events", "key", b"bad"))
    assert failure.value.attempts == 1
    assert attempts == 1


@pytest.mark.asyncio
async def test_worker_jitter_does_not_exceed_retry_backoff_cap(monkeypatch) -> None:
    attempts = 0
    delays: list[float] = []

    def handler(event: Event) -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError("transient failure")

    async def record_sleep(delay: float) -> None:
        delays.append(delay)

    worker = Worker(
        "worker",
        "group",
        ["events"],
        handler=handler,
        max_handler_retries=2,
        retry_backoff_base=10,
    )
    monkeypatch.setattr("meshflow.worker.process.random.uniform", lambda low, high: high)
    monkeypatch.setattr("meshflow.worker.process.asyncio.sleep", record_sleep)
    with pytest.raises(_HandlerFailureError):
        await worker._process_with_retries(Event.create("events", "key", b"payload"))
    assert attempts == 3
    assert delays == [2.0, 2.0]
