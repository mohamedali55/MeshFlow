"""Command-line entry point for MeshFlow scheduler experiments."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
from pathlib import Path
from typing import Any

from meshflow.benchmark.live import run_tcp_benchmark
from meshflow.benchmark.runner import run_benchmark
from meshflow.benchmark.workloads import SCENARIOS


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError("benchmark produced no raw samples")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


async def _run(args: argparse.Namespace) -> None:
    scenarios = SCENARIOS if args.scenario == "all" else (args.scenario,)
    if args.mode == "tcp":
        measurements = [
            await run_tcp_benchmark(
                scenario=scenario,
                events=args.events,
                partitions=args.partitions,
                workers=args.workers,
                seed=args.seed,
                payload_bytes=args.payload_bytes,
            )
            for scenario in scenarios
        ]
        report: dict[str, Any] = {
            "benchmark_type": "local_tcp_broker_measurement",
            "interpretation": (
                "Measured against a local single broker; results do not represent "
                "multi-process cluster or production hardware performance."
            ),
            "configuration": {
                "mode": "tcp",
                "scenarios": list(scenarios),
                "events": args.events,
                "partitions": args.partitions,
                "workers_in_scenario": args.workers,
                "runs": 1,
                "seed": args.seed,
                "payload_bytes": args.payload_bytes,
                "python": measurements[0]["configuration"]["python"],
                "platform": measurements[0]["configuration"]["platform"],
                "logical_cpu_count": measurements[0]["configuration"]["logical_cpu_count"],
            },
            "raw_runs": [
                sample for measurement in measurements for sample in measurement["raw_runs"]
            ],
        }
    else:
        report = run_benchmark(
            scenarios=scenarios,
            events=args.events,
            partitions=args.partitions,
            workers=args.workers,
            runs=args.runs,
            seed=args.seed,
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    csv_path = args.output.with_suffix(".csv")
    _write_csv(csv_path, report["raw_runs"])
    print(f"Wrote {len(report['raw_runs'])} raw samples to {args.output} and {csv_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run reproducible MeshFlow benchmarks")
    parser.add_argument("--mode", choices=("simulation", "tcp"), default="simulation")
    parser.add_argument("--scenario", choices=("all", *SCENARIOS), default="all")
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--output", type=Path, default=Path("benchmark-results.json"))
    asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    main()
