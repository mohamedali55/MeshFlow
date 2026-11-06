"""Portable process resource metrics for MeshFlow service processes."""

from __future__ import annotations

from dataclasses import dataclass

import psutil


@dataclass(frozen=True, slots=True)
class ProcessMetrics:
    cpu_percent: float
    memory_percent: float


class ProcessMetricsCollector:
    """Samples process CPU and memory usage without blocking the event loop."""

    def __init__(self) -> None:
        self._process = psutil.Process()
        self._process.cpu_percent(interval=None)

    def sample(self) -> ProcessMetrics:
        return ProcessMetrics(
            cpu_percent=min(100.0, max(0.0, self._process.cpu_percent(interval=None))),
            memory_percent=min(100.0, max(0.0, self._process.memory_percent())),
        )
