# MeshFlow Benchmarks and Failure Experiments

The benchmark package provides two complementary modes:

1. A deterministic scheduler simulation comparing static, round-robin,
   least-loaded, and adaptive placement on the same seeded workload.
2. A local TCP measurement that runs the real broker and uses the public
   producer, consumer, and commit APIs.

Neither mode is a substitute for a multi-host production benchmark. Simulation
values are estimated; TCP results are from one broker in the benchmark process
on the current machine. Reports state which mode produced each result.

## Scheduler simulation

Run all baseline scenarios and save JSON plus raw CSV samples:

```powershell
python -m meshflow.benchmark --scenario all --events 10000 --partitions 16 --workers 4 --runs 5 --seed 41 --output .\results\schedulers.json
```

Scenarios are `uniform`, `uneven`, `spike`, `slow_worker`, `worker_failure`,
and `hot_partition`. All schedulers receive identical partition counts and
worker capacities for each run. One deterministic warm-up run is excluded from
measurements. The report includes raw per-run throughput, estimated p50/p95/p99
latency, lag, utilization variance, simulated rebalance/recovery costs, and
median plus median absolute deviation across repetitions.
The JSON report also includes an `evaluation` section comparing adaptive
placement with each baseline for throughput, p95 latency, and lag. Positive
percentages mean adaptive performed better for that metric. These comparisons
are simulation results, not production guarantees.

Simulation does not model network, disk, real CPU or memory consumption, or
handler side effects. Simulated duplicate/lost counts are zero by construction
and are not correctness claims about a live cluster. Recovery and migration
durations are model estimates, not measured durations.

## Local TCP broker measurement

Exercise publish, fetch, and commit over real TCP sockets:

```powershell
python -m meshflow.benchmark --mode tcp --scenario uneven --events 5000 --partitions 8 --workers 4 --seed 41 --payload-bytes 256 --output .\results\tcp.json
```

The benchmark starts a broker on an ephemeral localhost port in a temporary
directory, warms up with one excluded event, publishes the configured workload,
then fetches and commits every record. It reports measured publish/consume
throughput, publish latency percentiles, event counts, lag, runner-process CPU
and memory deltas, and process RSS/VMS. The broker runs in the same process as
the benchmark driver, so resource readings are not broker-only readings.
Temporary broker data is removed when the benchmark exits.

Both modes produce a JSON report and a sibling CSV containing the raw samples.
Record the command, seed, machine, Python version, and any failed runs when
sharing results. Compare medians and variation, not only the best run.

## Failure injection

Failure helpers are explicit and bounded:

- `FailureInjector.with_network_delay` delays one supplied async operation.
- `FailureInjector.terminate_process` terminates a supplied child-process handle.
- `FailureInjector.corrupt_log` flips one byte only under a caller-supplied root.
- `BoundedDiskWriter` simulates `ENOSPC` in memory without filling a filesystem.

The test suite injects `ENOSPC` at the broker segment-write boundary and
verifies that the publish fails and the partition becomes read-only. It also
verifies that injected record corruption is detected during broker recovery.
No helper fills a real disk or targets an arbitrary process ID. Run destructive
experiments only against disposable data directories.

## Reproducibility limits

The local TCP mode measures one run per invocation; repeat the command to
collect independent runs. Keep event count, payload size, partition count,
filesystem, power mode, and background load the same when comparing revisions.
The scheduler simulator is useful for policy comparisons under controlled
assumptions, but adaptive simulation results do not prove that the distributed
optimizer will produce the same improvement on real workloads.
