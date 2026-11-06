"""Pressure normalization, smoothing, and explainable optimization decisions."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Any


@dataclass(frozen=True, slots=True)
class PressureConfig:
    """Experimental policy thresholds; tune against representative workloads."""

    alpha: float = 0.2
    overloaded_enter: float = 0.85
    overloaded_exit: float = 0.65
    minimum_benefit: float = 0.15
    migration_cost: float = 0.05
    cooldown_seconds: float = 15.0
    migration_budget: int = 3
    budget_window_seconds: float = 60.0
    max_sample_age_seconds: float = 30.0

    def __post_init__(self) -> None:
        for name in (
            "alpha",
            "overloaded_enter",
            "overloaded_exit",
            "minimum_benefit",
            "migration_cost",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be finite")
            if not isfinite(value):
                raise ValueError(f"{name} must be finite")
        if not 0 < self.alpha <= 1:
            raise ValueError("alpha must be in (0, 1]")
        if not 0 <= self.overloaded_exit < self.overloaded_enter <= 1:
            raise ValueError("pressure hysteresis thresholds must satisfy 0 <= exit < enter <= 1")
        if not 0 <= self.minimum_benefit <= 1 or not 0 <= self.migration_cost <= 1:
            raise ValueError("benefit and migration cost must be in [0, 1]")
        if (
            self.cooldown_seconds < 0
            or self.budget_window_seconds <= 0
            or self.max_sample_age_seconds <= 0
            or isinstance(self.migration_budget, bool)
            or not isinstance(self.migration_budget, int)
            or self.migration_budget <= 0
        ):
            raise ValueError("cooldown, windows, and migration budget must be positive")


def normalized_pressure(
    *,
    cpu_percent: float,
    queue_depth: int,
    queue_capacity: int,
    p95_latency_ms: float,
    memory_percent: float,
    lag_records: int,
) -> tuple[dict[str, float], float]:
    """Normalize documented resource signals and return their weighted score."""
    if queue_capacity <= 0 or queue_depth < 0 or lag_records < 0:
        raise ValueError("queue capacity must be positive and queue depth/lag non-negative")
    values = {
        "cpu": cpu_percent / 100.0,
        "queue": queue_depth / queue_capacity,
        "latency": p95_latency_ms / 1000.0,
        "memory": memory_percent / 100.0,
        "lag": lag_records / 1000.0,
    }
    if any(not isfinite(value) or value < 0 for value in values.values()):
        raise ValueError("metric values must be finite and non-negative")
    normalized = {name: min(1.0, value) for name, value in values.items()}
    score = (
        0.25 * normalized["cpu"]
        + 0.25 * normalized["queue"]
        + 0.20 * normalized["latency"]
        + 0.15 * normalized["memory"]
        + 0.15 * normalized["lag"]
    )
    return normalized, score


def smooth_pressure(previous: float | None, sample: float, alpha: float) -> float:
    if not isfinite(sample) or not 0 <= sample <= 1:
        raise ValueError("pressure sample must be in [0, 1]")
    if previous is None:
        return sample
    if not isfinite(previous) or not 0 <= previous <= 1:
        raise ValueError("previous pressure must be in [0, 1]")
    if not isfinite(alpha) or not 0 < alpha <= 1:
        raise ValueError("alpha must be in (0, 1]")
    return alpha * sample + (1 - alpha) * previous


def validate_metrics(payload: dict[str, Any]) -> dict[str, float | int]:
    """Validate a metrics report and normalize its accepted scalar types."""
    fields: dict[str, tuple[type, float]] = {
        "cpu_percent": (float, 100.0),
        "memory_percent": (float, 100.0),
        "queue_depth": (int, 1_000_000_000),
        "queue_capacity": (int, 1_000_000_000),
        "p95_latency_ms": (float, 86_400_000.0),
        "processing_rate": (float, 1_000_000_000.0),
        "in_flight": (int, 1_000_000_000),
    }
    result: dict[str, float | int] = {}
    for field, (expected_type, maximum) in fields.items():
        value = payload.get(field)
        if (
            isinstance(value, bool)
            or (
                not isinstance(value, int)
                if expected_type is int
                else not isinstance(value, (int, float))
            )
            or not isfinite(value)
            or value < 0
            or value > maximum
            or (field == "queue_capacity" and value == 0)
        ):
            raise ValueError(f"{field} is outside its valid range")
        result[field] = int(value) if expected_type is int else float(value)
    return result
