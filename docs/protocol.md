# MeshFlow TCP Protocol

**Status:** Protocol version 1  
**Transport:** TCP streams, using Python `asyncio`  
**Encoding:** UTF-8 JSON envelopes with base64-encoded binary payloads

## Frame format

Each frame starts with a four-byte unsigned big-endian body length, followed by
that many bytes of UTF-8 JSON. The length excludes the prefix. The broker and
clients reject zero-length frames and bodies larger than `max_frame_bytes`
(4 MiB by default) before reading the body.

An envelope contains:

```json
{
  "version": 1,
  "type": "PUBLISH",
  "request_id": "unique-request-id",
  "sender_id": "producer-1",
  "deadline_ms": 1790000000000,
  "payload": {}
}
```

Protocol version, message type, request ID, sender ID, deadline, and JSON-object
payload are validated. Responses copy the request ID. Request deadlines use
Unix epoch milliseconds; expired requests receive `DEADLINE_EXCEEDED`. A
client timeout applies independently to connection setup, writes, and reads.
Each request uses one TCP connection; the connection is closed after its
response.

## Supported broker operations

### `PUBLISH` → `PUBLISH_ACK`

The request contains `topic`, `partition_id`, and `event`. Event payload bytes
are base64 encoded. The broker verifies that the partition is in range and
matches the stable key hash, then returns the assigned `offset`.

The producer and broker must use the same configured partition count for each
topic. If not specified, both default to one partition.
Publishing the same `event_id` again to the same partition returns the
original offset and does not append another record. The first stored record
wins; producers must retain the same event ID when retrying an ambiguous
publish. Deduplication is partition-local and lasts as long as the log record
is retained. Brokers keep a disk-backed derived index and rebuild it from the
log during recovery, rather than retaining every event ID in process memory.

### `FETCH` → `FETCH_RESPONSE`

The request contains `topic`, `partition_id`, and `max_records` (1–1000), with
optional `offset` and `group_id`. Omitting `offset` starts at the group's
committed next offset, or zero when no offset has been committed. The response
includes records, `start_offset`, `next_offset`, and `committed_offset`.
It also includes nullable `checkpoint_offset` and base64
`checkpoint_state` fields when the group has a saved application checkpoint.
Coordinated brokers additionally require `group_id` and `assignment_epoch`;
the worker identity is the envelope's `sender_id`.

### `COMMIT_OFFSET` → `COMMIT_ACK`

The request contains `group_id`, `topic`, `partition_id`, and `next_offset`.
Offsets mean the next record to fetch. The broker rejects commits beyond the
partition end or behind the previous commit. It writes offset metadata to a
temporary file, flushes and fsyncs it, then atomically replaces the metadata
file before acknowledging. The consumer must only commit after processing
succeeds; concurrent consumers must commit only the highest contiguous
completed offset. Coordinated brokers also require the current
`assignment_epoch` and reject stale owners.

An optional base64 `checkpoint_state` (at most 1 MiB decoded) is written in
the same atomic metadata replacement as `next_offset`. The checkpoint is
associated with that exact offset and can be retrieved on a later fetch.
Committing without checkpoint state clears the previous application state.
This local checkpoint does not make arbitrary application side effects
transactional.

If a persisted group offset is beyond the recovered end of a partition, fetch
returns `COMMITTED_OFFSET_OUT_OF_RANGE` rather than silently skipping data.

## Coordinator membership and rebalancing

The coordinator serves `REGISTER_NODE`, `HEARTBEAT`, `JOIN_GROUP`,
`GET_ASSIGNMENTS`, `ACK_REVOKE`, `LEAVE_GROUP`, and topic/partition metadata
requests. `METRICS_REPORT` accepts node resource/workload samples and
per-assignment consumer lag; `GET_METRICS`, `OPTIMIZE_GROUP`, and
`GET_DECISIONS` expose current measurements and optimizer history. Brokers use
`VALIDATE_ASSIGNMENT` internally before group fetch and commit operations.

Metric reports include a process `instance_id`, sample interval, CPU and memory
percent, queue depth/capacity, p95 latency, processing rate, and in-flight work.
Worker reports also include assigned partition lag. The coordinator retains
raw samples, normalized components, and exponentially smoothed pressure for
diagnostics.

Nodes progress through `STARTING`, `HEALTHY`, `SUSPECTED`, and `UNAVAILABLE`.
Workers heartbeat periodically. A rebalance marks current assignments
`REVOKING`; brokers then reject new fetches while permitting the old owner to
commit in-flight work at its current epoch. The worker acknowledges revocation
after draining its current batch. The coordinator advances the epoch before
activating the next assignment, so requests with an older epoch receive
`STALE_EPOCH`. Heartbeat expiry removes unavailable workers after the
configured suspected grace period.

Abrupt process loss can happen after an application side effect but before its
offset commit. Such records may be delivered again; handlers should be
idempotent.

## Errors and retries

Invalid requests return `ERROR` with `code`, `message`, and `retryable`.
Capacity rejection returns `OVERLOADED` with `accepted: false` and a
`retry_after_ms` hint. Storage `ENOSPC` failures return `DISK_FULL`, and the
affected partition is marked read-only until restart/recovery. Clients retry
overload and retryable failures using capped exponential backoff and jitter.
Producers bound simultaneous publishes with `max_in_flight` (100 by default).
Event-ID deduplication makes an ambiguous publish retry safe within the routed
partition.

Workers retry handler exceptions with bounded exponential backoff. Handlers
can raise `PermanentHandlerError` to bypass retries for a known-permanent
failure. By default, after three retries the original event is published to `<source-topic>.DLQ`
and the source offset is committed only after the DLQ publish is acknowledged.
The coordinator creates the DLQ topic with one partition on first use. DLQ
headers retain the original topic, partition, offset, failure, attempt count,
and failure timestamp. If DLQ publishing fails, the worker leaves the source
offset uncommitted. Set `--no-dead-letter` to disable DLQ publishing; exhausted
handler failures then stop the worker iteration without committing the record.

## Security and operational limits

The protocol has no authentication or encryption. Bind to a trusted interface
only. Do not expose this development broker to untrusted networks.

Frames, record sizes, active connections, fetch batch sizes, and socket
timeouts are bounded. Brokers serve `PUBLISH`, `FETCH`, and `COMMIT_OFFSET`;
coordinators serve the membership and metadata operations above. Unsupported
message types receive an explicit `UNSUPPORTED_MESSAGE` error.
