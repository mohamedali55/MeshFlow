# Running a MeshFlow Development Cluster

MeshFlow provides SQLite-backed coordination, broker placement, consumer-group
assignments, heartbeat-based membership, epoch fencing, and independently
running worker processes. This is a development cluster, not a production
deployment.

## Start processes

Start one coordinator:

```powershell
python -m meshflow.coordinator.server --database .\data\coordinator.sqlite --port 9501 --heartbeat-timeout 15 --suspect-grace 5
```

Start three brokers in separate terminals, each with a unique ID and data
directory:

```powershell
python -m meshflow.broker.server --broker-id broker-1 --data-dir .\data --port 9500 --coordinator-host 127.0.0.1
python -m meshflow.broker.server --broker-id broker-2 --data-dir .\data --port 9502 --coordinator-host 127.0.0.1
python -m meshflow.broker.server --broker-id broker-3 --data-dir .\data --port 9503 --coordinator-host 127.0.0.1
```

Create a topic after the brokers register:

```python
import asyncio

from meshflow.coordinator import CoordinatorClient

async def main():
    coordinator = CoordinatorClient("127.0.0.1", 9501, sender_id="admin")
    partitions = await coordinator.create_topic("orders", 12)
    print(partitions)

asyncio.run(main())
```

The coordinator assigns partitions round-robin across registered healthy
brokers. Use the coordinator-aware producer and consumer options to route each
partition to its broker:

```python
from meshflow.producer import Producer
from meshflow.consumer import Consumer

producer = Producer(
    "127.0.0.1", 9500, coordinator_host="127.0.0.1", coordinator_port=9501
)
consumer = Consumer(
    "127.0.0.1", 9500, group_id="order-workers",
    coordinator_host="127.0.0.1", coordinator_port=9501,
)
```

For direct consumer use on a coordinated broker, join the group first and
provide the assignment's `worker_id` as `sender_id` and its `epoch` as
`assignment_epoch` on every `fetch` and `commit`. Coordinated brokers reject
group fetches without an active worker assignment and epoch. The `Worker` class
handles this automatically.

Start one or more worker processes:

```powershell
python -m meshflow.worker.process --worker-id worker-1 --group-id order-workers --topics orders --coordinator-port 9501
python -m meshflow.worker.process --worker-id worker-2 --group-id order-workers --topics orders --coordinator-port 9501
```

Workers register, join the group, poll assigned partitions, process each batch
sequentially, then commit the highest successfully completed next offset.
Pass a handler to the `Worker` API to run application logic; the CLI's default
handler logs processed event metadata. Workers heartbeat every five seconds;
the coordinator defaults to a 15-second heartbeat timeout followed by a
five-second suspected grace period. Configure these coordinator thresholds to
match expected scheduling and network delays.

## Membership and safe rebalancing

- Nodes move through `STARTING`, `HEALTHY`, `SUSPECTED`, and `UNAVAILABLE`.
  Workers in `SUSPECTED` are removed from new fetches while their in-flight
  work can still commit during revocation. After the grace period, unavailable
  workers are removed from group membership and their old epochs are fenced.
- A graceful rebalance blocks new fetches for revoking partitions, waits for
  the worker to finish and commit its current batch, accepts its revocation
  acknowledgement, then advances the epoch before publishing the new
  assignment. Brokers validate group fetch and commit epochs against the
  coordinator.
- Abrupt worker loss cannot reveal whether an application handler completed a
  side effect before the worker failed. Delivery is therefore at-least-once;
  handlers should be idempotent.
- Heartbeat timeouts can mistake a slow or partitioned worker for a failure.
  Detection thresholds are configurable, but this phase does not implement
  quorum-based failure detection.
- The coordinator remains a single point of failure; SQLite persistence does
  not provide consensus or failover.
- Broker partition placement is static after topic creation. There is no
  replication or automatic broker-failure recovery.
- Worker capacity is a relative weight used by the greedy assignment baseline;
  partition workloads are treated as uniform.
- The protocol has no authentication or encryption. Keep the cluster on a
  trusted network.

## Reliability: retries, checkpoints, and overload

Workers retry handler exceptions up to three times by default, with capped
exponential backoff and jitter. Once attempts are exhausted, the original
event is published to `<topic>.DLQ`; that topic is created with one partition
on first use. The DLQ record retains the original event ID and payload and adds
headers for source topic/partition/offset, total attempts, failure time, and
error text. The source offset advances only after the DLQ publish succeeds.
Use `--max-handler-retries` and `--retry-backoff-base` to tune retries, or
`--no-dead-letter` to leave a failing event uncommitted after retries.
Application handlers can raise `PermanentHandlerError` to bypass retries and
send a known-permanent failure directly to the DLQ.

Handler side effects can occur before a crash or a failed offset commit, so
delivery remains at-least-once. Make application handlers idempotent using the
stable event ID. Broker publish retries with the same event ID are deduplicated
per partition; the original persisted event wins. Deduplication lasts for the
retained lifetime of the partition log.

`Consumer.commit` can atomically persist application checkpoint bytes with the
offset:

```python
await consumer.commit(
    "orders", partition_id, next_offset, checkpoint_state=b"state-at-next-offset"
)
restored = await consumer.fetch("orders", partition_id)
if restored.checkpoint_state is not None:
    restore_application_state(restored.checkpoint_state)
```

Checkpoint state is limited to 1 MiB and stored locally alongside broker
offset metadata. It is not a transaction with external databases or services.
Commits without state clear the previous checkpoint.

Broker overload responses explicitly say a request was not accepted and
provide a retry delay. Producer clients cap concurrent publishes at 100 by
default and retry overload responses within their configured retry budget.

## Metrics and adaptive optimization

  Brokers and workers report process CPU and memory utilization, queue depth and
  capacity, p95 request/handler latency, processing rate, in-flight work, and
  worker partition lag. Samples include a process-instance ID and measurement
  interval. The coordinator stores raw values, normalized pressure components,
  an EMA score, and the overload hysteresis state.

  The initial score uses these normalized components, clamped to `[0, 1]`:

  | Component | Normalization | Weight |
  |---|---|---:|
  | CPU | `cpu_percent / 100` | 0.25 |
  | Queue | `queue_depth / queue_capacity` | 0.25 |
  | p95 latency | `p95_latency_ms / 1000` | 0.20 |
  | Memory | `memory_percent / 100` | 0.15 |
  | Consumer lag | `lag_records / 1000` | 0.15 |

  The default EMA alpha is `0.2`. A node enters overload at pressure `0.85` and
  leaves it below `0.65`. The optimizer proposes one highest-lag eligible
  partition move from an overloaded worker to a healthy worker below the exit
  threshold, provided capacity, benefit, cooldown, and migration-budget checks
  pass. The move then uses the safe Phase 4 revocation and epoch fencing flow.
  The experimental thresholds can be tuned with coordinator CLI options such
  as `--pressure-alpha`, `--overloaded-enter`, `--overloaded-exit`,
  `--minimum-benefit`, `--migration-cost`, `--cooldown-seconds`, and
  `--migration-budget`.

  Inspect current samples and decision history from Python:

  ```python
  metrics = await coordinator.metrics()
  snapshot = await coordinator.cluster_status()
  decisions = await coordinator.decisions("order-workers")
  ```

  `cluster_status()` returns nodes, current metrics, topics, and assignment
  counts in one read-only snapshot for dashboards and incident inspection.
  Each decision has an explanation and raw/normalized supporting measurements.

## Coordinator restart and recovery

Coordinator metadata is stored in SQLite with WAL mode and `synchronous=FULL`.
Restarting the coordinator with the same database path reconstructs registered
nodes, topic placement, group assignments, health state, metrics, and optimizer
decisions. Clients should re-register and heartbeat after a restart; broker and
worker processes retry their periodic heartbeats, but SQLite persistence is not
a consensus log and does not provide active failover.
  After a move, the coordinator compares the mean source/destination pressure
  before and after fresh samples arrive, and records actual benefit and
  prediction error. `NO_ACTION` decisions explain which threshold or safety
  condition prevented a move.

  This is a baseline policy, not a learned optimizer. The score weights,
  normalization targets, and migration-cost estimate are experimental; tune them
  against representative workloads. A single hot partition cannot be split,
  and adaptive concurrency control is not implemented. Abrupt worker loss
  remains at-least-once, so handlers should be idempotent.
