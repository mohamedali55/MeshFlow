# MeshFlow Scheduler Evaluation Report

## Executive summary

This report records a deterministic scheduler comparison using the benchmark
runner shipped with MeshFlow. Adaptive scheduling improves materially in
`slow_worker`, `spike`, and `hot_partition` scenarios, but it is not uniformly
better: `least_loaded` wins the `uneven` scenario in this sample. That result
is useful evidence against claiming that the optimizer is universally optimal.

## Reproduction

```powershell
python -m meshflow.benchmark `
  --scenario all `
  --events 1000 `
  --partitions 8 `
  --workers 4 `
  --runs 3 `
  --seed 41 `
  --output .\results\meshflow-scheduler-report.json
```

The run produced 72 raw samples: six scenarios, four schedulers, and three
repetitions per scheduler. Summaries use medians across repetitions. The
simulation is deterministic and does not claim to measure network, disk, or
handler performance.

## Median results

| Scenario | Scheduler | Throughput | P95 latency | Estimated lag |
|---|---:|---:|---:|---:|
| hot_partition | adaptive | 1.43 | 650.05 | 140 |
| hot_partition | least_loaded | 1.43 | 650.05 | 140 |
| hot_partition | round_robin | 1.35 | 693.05 | 148 |
| hot_partition | static | 1.35 | 693.05 | 148 |
| slow_worker | adaptive | 2.67 | 350.05 | 150 |
| slow_worker | least_loaded | 2.00 | 360.05 | 25 |
| slow_worker | round_robin | 1.00 | 800.20 | 50 |
| slow_worker | static | 1.00 | 800.20 | 50 |
| spike | adaptive | 1.70 | 538.05 | 117 |
| spike | least_loaded | 1.70 | 538.05 | 117 |
| spike | round_robin | 1.55 | 597.05 | 129 |
| spike | static | 1.55 | 597.05 | 129 |
| uneven | adaptive | 3.50 | 255.05 | 118 |
| uneven | least_loaded | 3.57 | 240.00 | 104 |
| uneven | round_robin | 2.94 | 290.05 | 72 |
| uneven | static | 2.68 | 323.05 | 125 |

Uniform and worker-failure scenarios converge because their generated
constraints leave little placement freedom. Throughput is expressed in the
simulator's time units; latency and lag are estimates, not wall-clock
measurements.

## Interpretation

The report's JSON `evaluation` section computes adaptive improvement against
static, round-robin, and least-loaded baselines for throughput, p95 latency,
and lag. Positive percentages indicate an adaptive advantage for that metric.
Use the raw CSV and repeated runs when comparing code changes; do not compare
only the best run.
