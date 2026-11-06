from __future__ import annotations

import errno

import pytest

from meshflow.benchmark import SCENARIOS, generate_workload, run_benchmark
from meshflow.benchmark.failures import BoundedDiskWriter, FailureInjector


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_workload_generators_are_reproducible_and_preserve_event_count(scenario: str) -> None:
    first = generate_workload(scenario, partitions=8, workers=4, events=1_003, seed=77)
    second = generate_workload(scenario, partitions=8, workers=4, events=1_003, seed=77)
    assert first == second
    assert first.event_count == 1_003
    assert len(first.partition_events) == 8
    assert len(first.worker_capacity) == 4
    if scenario == "worker_failure":
        assert first.worker_capacity[-1] == 0


def test_benchmark_matches_schedulers_on_seeded_scenarios() -> None:
    report = run_benchmark(
        scenarios=("uneven", "worker_failure"),
        events=1_000,
        partitions=8,
        workers=3,
        runs=2,
        seed=10,
    )
    assert report["benchmark_type"] == "deterministic_scheduler_simulation"
    assert len(report["raw_runs"]) == 16
    assert len(report["summary"]) == 8
    for scenario in ("uneven", "worker_failure"):
        for run in range(2):
            samples = [
                row
                for row in report["raw_runs"]
                if row["scenario"] == scenario and row["run"] == run
            ]
            assert {row["scheduler"] for row in samples} == {
                "static",
                "round_robin",
                "least_loaded",
                "adaptive",
            }
            assert len({row["seed"] for row in samples}) == 1
            assert all(row["events"] == 1_000 for row in samples)


def test_failure_helpers_are_bounded_and_scope_checked(tmp_path) -> None:
    writer = BoundedDiskWriter(3)
    assert writer.write(b"ab") == 2
    with pytest.raises(OSError) as exc_info:
        writer.write(b"cd")
    assert exc_info.value.errno == errno.ENOSPC
    assert writer.data == b"ab"

    target = tmp_path / "segment.log"
    target.write_bytes(b"abcdef")
    FailureInjector.corrupt_log(target, root=tmp_path, offset=2)
    assert target.read_bytes() == b"abbdef"
    with pytest.raises(ValueError, match="inside"):
        FailureInjector.corrupt_log(target, root=tmp_path / "other", offset=0)
