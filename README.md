# MeshFlow

**A small, educational event-processing system for learning distributed
systems by building one.**

MeshFlow is a hands-on Python project that explores the ideas behind systems
such as Kafka-like brokers and worker queues. Instead of hiding the difficult
parts behind a framework, it makes them visible: durable logs, TCP framing,
consumer groups, rebalancing, retries, metrics, optimization, and
leader/follower replication.

The goal is not to provide a production broker. The goal is to make the
trade-offs understandable, testable, and easy to experiment with.

## Why this project exists

Distributed systems become easier to understand when each guarantee has a
concrete implementation and a failure test behind it. MeshFlow is organized
around questions such as:

- What happens if a process stops after handling an event but before committing
  its offset?
- How does a consumer know that an old assignment is no longer valid?
- What does a broker recover after a partial disk write?
- How can a coordinator observe pressure and move work safely?
- What should a leader do when a synchronous follower disappears?

Each phase of the project adds one of these ideas and documents its limits.

## A simple mental model

1. A **producer** creates an event and publishes it to a topic.
2. A stable partition key maps the event to one partition.
3. A **broker** appends the event to a durable, segmented log.
4. A **worker** receives an assignment from the coordinator, fetches events,
   runs application code, and commits the next offset only after processing.
5. The **coordinator** tracks brokers and workers, epochs assignments, records
   metrics, and can initiate safe partition movement.
6. For replicated topics, the partition leader forwards each append to its
   followers and acknowledges the producer only after they acknowledge.

## Architecture

```text
                         Control plane
                 +----------------------------+
                 | Coordinator + Optimizer    |
                 | membership, routing,      |
                 | epochs, metrics, decisions |
                 +-------------+--------------+
                               |
                 assignments, heartbeats, routes
                               |
 Producers -----> Leader broker ---------> Workers
                    |       |
                    |       +---- synchronous follower brokers
                    |
                    +---- durable partition logs
```

The coordinator stores cluster metadata in SQLite. Brokers store partition
logs, event-ID indexes, offsets, and checkpoints locally. Clients communicate
over bounded versioned TCP frames.

## What you can learn from MeshFlow

- **Storage:** Versioned binary records, CRC32 validation, segmented logs,
  sparse indexes, fsync policies, recovery, and damaged-partition handling.
- **Networking:** Bounded JSON frames, deadlines, request validation, retries,
  overload responses, and explicit error codes.
- **Coordination:** Broker placement, consumer groups, capacity-weighted
  assignments, heartbeats, revocations, and assignment-epoch fencing.
- **Reliability:** At-least-once delivery, bounded handler retries, dead-letter
  topics, atomic checkpoints, idempotent publish retries, and failure tests.
- **Optimization:** CPU, memory, queue, latency, and lag telemetry; normalized
  pressure; EMA smoothing; hysteresis; migration budgets; and outcome checks.
- **Replication:** Educational synchronous leader/follower replication with
  exact follower offsets and explicit follower-loss behavior.
- **Experiments:** Repeatable scheduler simulations and real local TCP
  measurements with JSON and CSV reports.

## Requirements

- Python 3.11 or newer
- Dependencies listed in `pyproject.toml`

## Install

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Run the tests

```powershell
pytest
ruff check src/ tests/
ruff format --check src/ tests/
```

The test suite covers storage recovery, protocol validation, worker retries,
concurrent producers and consumers, interrupted writes, simulated disk
exhaustion, stale assignments, optimizer behavior, coordinator recovery, and
replicated publishing.

## Run a local cluster

Start the coordinator and a broker in separate terminals:

```powershell
python -m meshflow.coordinator.server --database .\data\coordinator.sqlite --port 9501
python -m meshflow.broker.server --broker-id broker-1 --data-dir .\data --port 9500 --coordinator-host 127.0.0.1 --coordinator-port 9501
```

Create a topic:

```python
import asyncio

from meshflow.coordinator import CoordinatorClient


async def main():
    coordinator = CoordinatorClient(
        "127.0.0.1", 9501, sender_id="local-admin"
    )
    await coordinator.create_topic("events", 4)


asyncio.run(main())
```

Start a worker:

```powershell
python -m meshflow.worker.process --worker-id worker-1 --group-id event-workers --topics events --coordinator-host 127.0.0.1 --coordinator-port 9501
```

For multi-broker operation, rebalancing, heartbeats, metrics, and recovery
procedures, see the [cluster guide](docs/cluster.md).

## Try the producer and consumer APIs

```python
import asyncio

from meshflow.common.types import Event
from meshflow.consumer import Consumer
from meshflow.coordinator import CoordinatorClient
from meshflow.producer import Producer


async def main():
    producer = Producer(
        "127.0.0.1",
        9500,
        coordinator_host="127.0.0.1",
        coordinator_port=9501,
    )
    position = await producer.publish(
        Event.create("events", "customer-42", b'{"type":"created"}')
    )
    print("published:", position)

    coordinator = CoordinatorClient(
        "127.0.0.1", 9501, sender_id="example-reader"
    )
    await coordinator.register_node("example-reader", "worker", "127.0.0.1", 0)
    await coordinator.heartbeat("example-reader")
    assignments = await coordinator.join_group(
        "api-example-readers", "example-reader", ["events"]
    )
    assignment = next(
        item
        for item in assignments
        if item["partition_id"] == position.partition_id
    )

    consumer = Consumer(
        "127.0.0.1",
        9500,
        group_id="api-example-readers",
        sender_id="example-reader",
        coordinator_host="127.0.0.1",
        coordinator_port=9501,
    )
    result = await consumer.fetch(
        "events",
        position.partition_id,
        assignment_epoch=assignment["epoch"],
    )
    for event in result.events:
        print(event.event_id, event.payload)
    if result.records:
        await consumer.commit(
            "events",
            position.partition_id,
            result.records[-1].offset + 1,
            assignment_epoch=assignment["epoch"],
        )


asyncio.run(main())
```

The assignment epoch is important: it prevents a worker that lost ownership
during a rebalance from fetching or committing as if it were still current.
The `Worker` class handles this lifecycle automatically.

## Educational replication

Create a replicated topic only after starting at least two healthy brokers:

```python
await coordinator.create_topic(
    "replicated-events",
    6,
    replication_factor=2,
)
```

For every partition, the coordinator selects one leader and ordered followers.
Clients write to the leader. The leader appends locally, sends an exact-offset
append to every follower, and returns success only after all followers
acknowledge.

If a configured follower disappears, new publishes fail with
`REPLICATION_UNAVAILABLE` rather than reporting false durability. Existing
leader data remains readable, and a restarted follower can recover its local
log. This educational implementation does **not** provide quorum consensus,
automatic leader election, or automatic replica repair.

## Delivery guarantees

MeshFlow provides **at-least-once** processing. It does not provide exactly-once
external side effects. A handler can complete an external action and fail
before its offset commit, so application handlers should be idempotent and use
the stable event ID when deduplicating side effects.

Workers retry handler failures a bounded number of times. Exhausted failures
are published to `<topic>.DLQ` with source provenance by default.
`PermanentHandlerError` skips retries for a known-permanent failure. If DLQ
publishing fails, the source offset remains uncommitted.

Applications can persist up to 1 MiB of checkpoint state atomically alongside
an offset commit. Checkpoints and offsets are local broker metadata; they are
not transactions with an external database.

## Observability

The coordinator exposes a read-only `cluster_status()` snapshot containing:

- Node health and broker/worker metrics
- Partition leaders and configured replicas
- Consumer lag by group and partition
- Assignment-state counts
- Recent optimizer decisions

This makes internal behavior inspectable without adding a separate monitoring
service. See the [cluster guide](docs/cluster.md) for examples.

## Benchmarks

Compare static, round-robin, least-loaded, and adaptive scheduling:

```powershell
python -m meshflow.benchmark --scenario all --events 1000 --partitions 8 --workers 4 --runs 3 --seed 41 --output .\results\schedulers.json
```

Measure publish, fetch, and commit over real local TCP:

```powershell
python -m meshflow.benchmark --mode tcp --scenario hot_partition --events 5000 --partitions 8 --seed 41 --payload-bytes 256 --output .\results\tcp.json
```

Both modes write JSON and sibling CSV files. Simulation values are model
estimates; TCP measurements describe one broker on the local machine. The
comparative results and interpretation are documented in the
[benchmark evaluation report](docs/benchmark-report.md).

## Documentation

- [Cluster setup and operations](docs/cluster.md)
- [TCP protocol](docs/protocol.md)
- [Storage format and recovery](docs/storage-format.md)
- [Benchmarks and failure experiments](docs/benchmarks.md)
- [Scheduler evaluation report](docs/benchmark-report.md)

## Scope and limitations

MeshFlow is an educational development system, not a production deployment.
It has no authentication or encryption, and the coordinator has no consensus
or active failover. Replication is synchronous leader/follower replication,
not quorum replication. A follower failure blocks new writes for partitions
that require that follower until it recovers. Optimizer weights are
experimental, and a single hot partition cannot be split across workers.

Keep experiments on trusted networks and use disposable data directories.

## License

MeshFlow is distributed under the MIT License. See [LICENSE](LICENSE).
