# MeshFlow

**A hands-on distributed event-processing system built to make reliability,
coordination, and adaptive scheduling understandable.**

MeshFlow is an educational systems project. It implements the core pieces of an
event-processing platform—from a durable partitioned log and a TCP protocol to
consumer-group coordination, safe rebalancing, and a measurable optimization
policy—rather than wrapping an existing broker.

The project is designed to demonstrate how these components fit together and
how their failure modes affect correctness. It is not intended for production
deployment.

## What MeshFlow does

1. Producers publish events to partitioned topics. A stable key selects the
   partition, and brokers append records to local, segmented logs.
2. Consumers fetch records and commit offsets only after processing. Workers
   join groups and receive coordinator-managed partition assignments.
3. The coordinator tracks membership and assignment epochs. During a rebalance,
   workers drain and acknowledge revocations before ownership changes.
4. Brokers and workers report resource and workload metrics. An explainable
   policy can recommend and initiate safe partition moves, then compare the
   observed pressure change with its prediction.
5. Reliability tools provide bounded handler retries, dead-letter publishing,
   checkpoints, overload responses, and idempotent publish retries.

## Architecture

```text
                         Control plane
                +-----------------------------+
                | Coordinator + Optimizer     |
                | membership, routing, epochs |
                +----+-------------------+----+
                     ^ metrics/heartbeats |
                     |                    | assignments/revocations
                     |                    v
 Producers ------> Brokers ------------> Workers
                    | persistent logs     | fetch/process/commit
                    +---------------------+
```

The coordinator stores cluster metadata in SQLite. Brokers store each
partition's append-only log and local consumer offsets. Workers use the
coordinator to discover assignments and brokers, then communicate with brokers
over TCP.

## Capabilities

- **Storage:** Versioned binary records with CRC32, segmented append-only
  partition logs, sparse indexes, recovery, and damaged-log detection.
- **Protocol and clients:** Bounded, versioned TCP frames; asynchronous
  producer and consumer APIs; retries with exponential backoff and jitter.
- **Coordination:** Multi-broker topic placement, consumer groups,
  capacity-weighted assignments, heartbeats, and membership states.
- **Safe rebalancing:** Revoke/drain/acknowledge sequencing and assignment-epoch
  fencing for coordinated fetches and commits.
- **Adaptive optimization:** CPU, memory, queue, latency, and lag telemetry;
  normalized EMA pressure; explainable decisions; hysteresis, cooldown, and
  migration budgets; outcome verification.
- **Reliability:** Bounded handler retries, `PermanentHandlerError`, DLQ
  records with source provenance, atomic offset-plus-state checkpoints, and
  disk-backed event-ID deduplication rebuilt from the log.
- **Experiments:** Seeded workload scenarios and static, round-robin,
  least-loaded, and adaptive scheduler comparisons, plus a local TCP broker
  benchmark and bounded failure-injection helpers.

## Requirements

- Python 3.11 or newer
- The project dependencies listed in `pyproject.toml`

## Install

From the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

## Run the tests and checks

```powershell
pytest
ruff check src/ tests/
ruff format --check src/ tests/
```

Or use the Makefile targets where `make` is available:

```sh
make test
make lint
```

## Start a development cluster

Run each service in a separate terminal. These commands start a coordinator
and one broker on localhost:

```powershell
python -m meshflow.coordinator.server --database .\data\coordinator.sqlite --port 9501
python -m meshflow.broker.server --broker-id broker-1 --data-dir .\data --port 9500 --coordinator-host 127.0.0.1 --coordinator-port 9501
```

Create a topic after the broker has registered:

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

Start a worker in another terminal:

```powershell
python -m meshflow.worker.process --worker-id worker-1 --group-id event-workers --topics events --coordinator-host 127.0.0.1 --coordinator-port 9501
```

For multi-broker setup, producer/consumer examples, heartbeat and rebalance
details, and operational caveats, see the [cluster guide](docs/cluster.md).

## Producer and consumer APIs

The asynchronous `Producer` and `Consumer` APIs are available from
`meshflow.producer` and `meshflow.consumer`:

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
    print(position)

    coordinator = CoordinatorClient(
        "127.0.0.1", 9501, sender_id="example-reader"
    )
    await coordinator.register_node(
        "example-reader", "worker", "127.0.0.1", 0
    )
    await coordinator.heartbeat("example-reader")
    assignments = await coordinator.join_group(
        "api-example-readers", "example-reader", ["events"]
    )
    assignment = next(
        item
        for item in assignments
        if item["worker_id"] == "example-reader"
        and item["partition_id"] == position.partition_id
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

Coordinated worker requests must carry the active assignment epoch; the
`Worker` process handles this automatically. See the [protocol guide](docs/protocol.md)
for message framing and request details.

## Reliability and delivery guarantees

MeshFlow provides **at-least-once** processing, not exactly-once external side
effects. A handler can complete an external action and fail before its offset
commit, so handlers should be idempotent.

Workers retry failed handlers a bounded number of times, then publish the
original event with failure provenance to `<topic>.DLQ` by default. Applications
can raise `PermanentHandlerError` to bypass retries. If DLQ publishing fails,
the source offset remains uncommitted.

Applications can persist up to 1 MiB of checkpoint state atomically alongside
an offset commit. Checkpoints and consumer offsets are local to the broker and
do not include an external database transaction.

## Benchmarks

Run a deterministic scheduler simulation:

```powershell
python -m meshflow.benchmark --scenario uneven --events 10000 --partitions 16 --workers 4 --runs 5 --seed 41 --output .\results\schedulers.json
```

Measure publish, fetch, and commit using a temporary local TCP broker:

```powershell
python -m meshflow.benchmark --mode tcp --scenario hot_partition --events 5000 --partitions 8 --seed 41 --payload-bytes 256 --output .\results\tcp.json
```

Both commands write JSON and sibling CSV output. Simulation values are model
estimates; TCP measurements describe one broker on the local machine. They are
not production-cluster performance claims. See the [benchmark guide](docs/benchmarks.md)
for scenarios, metrics, limitations, and failure-injection details.

## Project documentation

- [Cluster setup and operations](docs/cluster.md)
- [TCP protocol](docs/protocol.md)
- [Storage format and recovery](docs/storage-format.md)
- [Benchmarks and failure experiments](docs/benchmarks.md)

## Limitations

MeshFlow is an educational, single-coordinator development system. It currently
has no replication, consensus, coordinator failover, authentication, or
encryption. A broker failure makes its local partitions unavailable until that
broker and its storage recover. Optimization policy weights are experimental,
and moving a partition cannot split a single hot partition. Keep deployments
on trusted networks and use disposable data for failure experiments.

## License

MeshFlow is distributed under the MIT License. See [LICENSE](LICENSE).
