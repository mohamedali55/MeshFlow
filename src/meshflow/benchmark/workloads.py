"""Seeded workload scenarios used by the benchmark simulator."""

from __future__ import annotations

import random
from dataclasses import dataclass

SCENARIOS = ("uniform", "uneven", "spike", "slow_worker", "worker_failure", "hot_partition")


@dataclass(frozen=True, slots=True)
class Workload:
    scenario: str
    partition_events: tuple[int, ...]
    worker_capacity: tuple[float, ...]
    seed: int

    @property
    def event_count(self) -> int:
        return sum(self.partition_events)


def generate_workload(
    scenario: str,
    *,
    partitions: int,
    workers: int,
    events: int,
    seed: int,
) -> Workload:
    """Create deterministic per-partition event counts and worker capacities."""
    if scenario not in SCENARIOS:
        raise ValueError(f"scenario must be one of {', '.join(SCENARIOS)}")
    if isinstance(partitions, bool) or not isinstance(partitions, int) or partitions <= 0:
        raise ValueError("partitions must be a positive integer")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers <= 0:
        raise ValueError("workers must be a positive integer")
    if isinstance(events, bool) or not isinstance(events, int) or events < 0:
        raise ValueError("events must be a non-negative integer")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed must be an integer")
    if scenario == "worker_failure" and workers < 2:
        raise ValueError("worker_failure scenario requires at least two workers")

    rng = random.Random(seed)
    if scenario == "uneven":
        weights = [rng.uniform(0.25, 2.0) for _ in range(partitions)]
    elif scenario == "spike":
        weights = [1.0] * partitions
        weights[0] = 10.0
    elif scenario == "hot_partition":
        weights = [0.3 / max(partitions - 1, 1)] * partitions
        weights[0] = 0.7 if partitions > 1 else 1.0
    else:
        weights = [1.0] * partitions

    total_weight = sum(weights)
    exact_counts = [events * weight / total_weight for weight in weights]
    counts = [int(value) for value in exact_counts]
    remaining = events - sum(counts)
    remainder_order = sorted(
        range(partitions),
        key=lambda index: (exact_counts[index] - counts[index], rng.random()),
        reverse=True,
    )
    for index in remainder_order[:remaining]:
        counts[index] += 1

    capacity = [1.0] * workers
    if scenario == "slow_worker":
        capacity[0] = 0.25
    elif scenario == "worker_failure" and workers > 1:
        capacity[-1] = 0.0
    return Workload(scenario, tuple(counts), tuple(capacity), seed)
