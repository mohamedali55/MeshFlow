"""Repeatable scheduler simulation with raw samples and robust summaries."""

from __future__ import annotations

import os
import platform
import statistics
import sys
from collections import defaultdict
from typing import Any

from meshflow.benchmark.schedulers import SCHEDULERS, Schedule
from meshflow.benchmark.workloads import SCENARIOS, Workload, generate_workload

_METRICS = (
    "throughput_events_per_time_unit",
    "p50_latency_time_units",
    "p95_latency_time_units",
    "p99_latency_time_units",
    "estimated_lag_records",
    "estimated_cpu_utilization_percent",
    "utilization_variance",
    "rebalance_duration_ms",
    "optimization_overhead_ms",
    "recovery_time_ms",
    "simulated_duplicate_events",
    "simulated_lost_events",
)


def _percentile(samples: list[float], percentile: float) -> float:
    if not samples:
        return 0.0
    ordered = sorted(samples)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _simulate(workload: Workload, schedule: Schedule) -> dict[str, float | int]:
    worker_count = len(workload.worker_capacity)
    worker_loads = [0.0] * worker_count
    for partition_id, owner in enumerate(schedule.owners):
        worker_loads[owner] += (
            workload.partition_events[partition_id] / workload.worker_capacity[owner]
        )
    makespan = max(worker_loads, default=0.0)
    latencies: list[float] = []
    sample_limit = 20_000
    stride = max(1, (workload.event_count + sample_limit - 1) // sample_limit)
    worker_elapsed = [0.0] * worker_count
    for partition_id, owner in enumerate(schedule.owners):
        speed = workload.worker_capacity[owner]
        event_count = workload.partition_events[partition_id]
        for event_index in range(0, event_count, stride):
            latencies.append(worker_elapsed[owner] + (event_index + 1) / speed)
        worker_elapsed[owner] += event_count / speed

    utilization = [min(load / makespan, 1.0) if makespan else 0.0 for load in worker_loads]
    measured_window = makespan * 0.8
    estimated_lag = sum(
        max(
            0,
            int((load - measured_window) * workload.worker_capacity[worker_id]),
        )
        for worker_id, load in enumerate(worker_loads)
    )
    throughput = workload.event_count / makespan if makespan else 0.0
    recovery_ms = 250.0 if workload.scenario == "worker_failure" else 0.0
    rebalance_ms = float(schedule.migrations * 25)
    return {
        "throughput_events_per_time_unit": throughput,
        "p50_latency_time_units": _percentile(latencies, 0.50),
        "p95_latency_time_units": _percentile(latencies, 0.95),
        "p99_latency_time_units": _percentile(latencies, 0.99),
        "estimated_lag_records": estimated_lag,
        "estimated_cpu_utilization_percent": (
            statistics.mean(utilization) * 100 if utilization else 0.0
        ),
        "utilization_variance": statistics.pvariance(utilization) if utilization else 0.0,
        "rebalance_duration_ms": rebalance_ms,
        "optimization_overhead_ms": float(schedule.migrations * 5),
        "recovery_time_ms": recovery_ms,
        "simulated_duplicate_events": 0,
        "simulated_lost_events": 0,
    }


def run_benchmark(
    *,
    scenarios: tuple[str, ...] = SCENARIOS,
    events: int = 10_000,
    partitions: int = 16,
    workers: int = 4,
    runs: int = 5,
    seed: int = 1,
) -> dict[str, Any]:
    """Run all requested scheduler/scenario pairs with matched seeded inputs."""
    if not scenarios or any(scenario not in SCENARIOS for scenario in scenarios):
        raise ValueError("scenarios must contain known workload names")
    if isinstance(runs, bool) or not isinstance(runs, int) or runs <= 0:
        raise ValueError("runs must be a positive integer")

    raw_runs: list[dict[str, Any]] = []
    for scenario_index, scenario in enumerate(scenarios):
        warmup = generate_workload(
            scenario,
            partitions=partitions,
            workers=workers,
            events=events,
            seed=seed + scenario_index * runs - 1,
        )
        for scheduler in SCHEDULERS.values():
            _simulate(warmup, scheduler(warmup))
        for run_index in range(runs):
            run_seed = seed + scenario_index * runs + run_index
            workload = generate_workload(
                scenario,
                partitions=partitions,
                workers=workers,
                events=events,
                seed=run_seed,
            )
            for scheduler in SCHEDULERS.values():
                schedule = scheduler(workload)
                raw_runs.append(
                    {
                        "scenario": scenario,
                        "scheduler": schedule.name,
                        "run": run_index,
                        "seed": run_seed,
                        "events": workload.event_count,
                        "partitions": partitions,
                        "workers": workers,
                        "migrations": schedule.migrations,
                        **_simulate(workload, schedule),
                    }
                )

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for sample in raw_runs:
        grouped[(sample["scenario"], sample["scheduler"])].append(sample)
    summary: list[dict[str, Any]] = []
    for (scenario, scheduler), samples in sorted(grouped.items()):
        row: dict[str, Any] = {
            "scenario": scenario,
            "scheduler": scheduler,
            "runs": len(samples),
        }
        for metric in _METRICS:
            values = [float(sample[metric]) for sample in samples]
            median = statistics.median(values)
            deviations = [abs(value - median) for value in values]
            row[f"{metric}_median"] = median
            row[f"{metric}_mad"] = statistics.median(deviations)
        summary.append(row)

    return {
        "benchmark_type": "deterministic_scheduler_simulation",
        "interpretation": (
            "Simulated scheduling results, not measured MeshFlow network throughput or "
            "hardware performance."
        ),
        "configuration": {
            "scenarios": list(scenarios),
            "events": events,
            "partitions": partitions,
            "workers": workers,
            "runs": runs,
            "warmup_runs": 1,
            "seed": seed,
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "logical_cpu_count": os.cpu_count(),
        },
        "raw_runs": raw_runs,
        "summary": summary,
    }
