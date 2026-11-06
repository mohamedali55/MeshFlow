"""Tests for normalized pressure and smoothing policy."""

from __future__ import annotations

import pytest

from meshflow.coordinator.optimizer import (
    PressureConfig,
    normalized_pressure,
    smooth_pressure,
    validate_metrics,
)


def test_normalized_pressure_weights_and_clamps_components() -> None:
    components, score = normalized_pressure(
        cpu_percent=100,
        queue_depth=200,
        queue_capacity=100,
        p95_latency_ms=2000,
        memory_percent=50,
        lag_records=500,
    )

    assert components == {
        "cpu": 1.0,
        "queue": 1.0,
        "latency": 1.0,
        "memory": 0.5,
        "lag": 0.5,
    }
    assert score == pytest.approx(0.85)


def test_pressure_ema_uses_configured_alpha() -> None:
    assert smooth_pressure(None, 0.8, 0.2) == pytest.approx(0.8)
    assert smooth_pressure(0.2, 0.8, 0.2) == pytest.approx(0.32)


@pytest.mark.parametrize(
    "values",
    [
        {"cpu_percent": float("nan")},
        {"cpu_percent": -1},
        {"queue_depth": -1},
        {"queue_capacity": 0},
        {"p95_latency_ms": float("inf")},
        {"in_flight": 1.5},
    ],
)
def test_metrics_validation_rejects_invalid_values(values) -> None:
    payload = {
        "cpu_percent": 1.0,
        "memory_percent": 1.0,
        "queue_depth": 0,
        "queue_capacity": 1,
        "p95_latency_ms": 0.0,
        "processing_rate": 0.0,
        "in_flight": 0,
    }
    payload.update(values)

    with pytest.raises(ValueError):
        validate_metrics(payload)


def test_pressure_config_rejects_invalid_hysteresis() -> None:
    with pytest.raises(ValueError, match="hysteresis"):
        PressureConfig(overloaded_exit=0.9, overloaded_enter=0.8)
