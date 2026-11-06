"""Simple baseline and adaptive partition schedulers for reproducible experiments."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from meshflow.benchmark.workloads import Workload


@dataclass(frozen=True, slots=True)
class Schedule:
    name: str
    owners: tuple[int, ...]
    migrations: int = 0


def _available_workers(workload: Workload) -> list[int]:
    return [index for index, capacity in enumerate(workload.worker_capacity) if capacity > 0]


def _block_static(workload: Workload) -> Schedule:
    workers = _available_workers(workload)
    block_size = (len(workload.partition_events) + len(workers) - 1) // len(workers)
    owners = tuple(
        workers[min(index // block_size, len(workers) - 1)]
        for index in range(len(workload.partition_events))
    )
    return Schedule("static", owners)


def _round_robin(workload: Workload) -> Schedule:
    workers = _available_workers(workload)
    return Schedule(
        "round_robin",
        tuple(workers[index % len(workers)] for index in range(len(workload.partition_events))),
    )


def _least_loaded(workload: Workload) -> Schedule:
    workers = _available_workers(workload)
    owner_loads = [0.0] * len(workload.worker_capacity)
    owners = [-1] * len(workload.partition_events)
    for partition_id in sorted(
        range(len(owners)),
        key=lambda index: (-workload.partition_events[index], index),
    ):
        owner = min(
            workers,
            key=lambda index: (owner_loads[index] / workload.worker_capacity[index], index),
        )
        owners[partition_id] = owner
        owner_loads[owner] += workload.partition_events[partition_id]
    return Schedule("least_loaded", tuple(owners))


def _makespan(workload: Workload, owners: list[int]) -> float:
    loads = [0.0] * len(workload.worker_capacity)
    for partition_id, owner in enumerate(owners):
        loads[owner] += workload.partition_events[partition_id] / workload.worker_capacity[owner]
    return max(loads, default=0.0)


def _adaptive(workload: Workload) -> Schedule:
    """Start from static placement and move partitions only when makespan improves."""
    owners = list(_block_static(workload).owners)
    migrations = 0
    while True:
        current_makespan = _makespan(workload, owners)
        best_move: tuple[float, int, int] | None = None
        for partition_id, source in enumerate(owners):
            for destination in _available_workers(workload):
                if destination == source:
                    continue
                candidate = owners.copy()
                candidate[partition_id] = destination
                candidate_makespan = _makespan(workload, candidate)
                if candidate_makespan < current_makespan * 0.95 and (
                    best_move is None or candidate_makespan < best_move[0]
                ):
                    best_move = (candidate_makespan, partition_id, destination)
        if best_move is None:
            break
        _, partition_id, destination = best_move
        owners[partition_id] = destination
        migrations += 1
    return Schedule("adaptive", tuple(owners), migrations)


SCHEDULERS: dict[str, Callable[[Workload], Schedule]] = {
    "static": _block_static,
    "round_robin": _round_robin,
    "least_loaded": _least_loaded,
    "adaptive": _adaptive,
}
