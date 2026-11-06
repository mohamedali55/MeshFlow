"""SQLite-backed cluster metadata and baseline worker assignment policy."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from math import isfinite
from pathlib import Path
from uuid import uuid4

from meshflow.coordinator.optimizer import (
    PressureConfig,
    normalized_pressure,
    smooth_pressure,
    validate_metrics,
)


@dataclass(frozen=True, slots=True)
class NodeRecord:
    node_id: str
    role: str
    host: str
    port: int
    capacity: float
    status: str
    last_heartbeat_ms: int


@dataclass(frozen=True, slots=True)
class PartitionAssignment:
    group_id: str
    topic: str
    partition_id: int
    worker_id: str
    broker_id: str
    broker_host: str
    broker_port: int
    epoch: int
    state: str


class AssignmentValidationError(Exception):
    """Raised when a broker request is not authorized by the current assignment."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class MetadataStore:
    """Persists coordinator metadata using short-lived SQLite connections."""

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def register_node(
        self, node_id: str, role: str, host: str, port: int, capacity: float = 1.0
    ) -> NodeRecord:
        self._validate_node(node_id, role, host, port, capacity)
        now = int(time.time() * 1000)
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT role, capacity FROM nodes WHERE node_id = ?", (node_id,)
            ).fetchone()
            if existing and existing["role"] != role:
                raise ValueError(f"Node {node_id!r} is already registered as {existing['role']}")
            capacity_changed = (
                role == "worker"
                and existing is not None
                and float(existing["capacity"]) != float(capacity)
            )
            connection.execute(
                """
                INSERT INTO nodes(node_id, role, host, port, capacity, status, last_heartbeat_ms)
                VALUES (?, ?, ?, ?, ?, 'HEALTHY', ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    host = excluded.host,
                    port = excluded.port,
                    capacity = excluded.capacity,
                    status = 'HEALTHY',
                    last_heartbeat_ms = excluded.last_heartbeat_ms
                """,
                (node_id, role, host, port, capacity, now),
            )
            connection.execute(
                """
                INSERT INTO node_health(node_id, status)
                VALUES (?, 'STARTING')
                ON CONFLICT(node_id) DO UPDATE SET status = 'STARTING'
                """,
                (node_id,),
            )
            if capacity_changed:
                groups = connection.execute(
                    "SELECT group_id FROM group_members WHERE worker_id = ?", (node_id,)
                ).fetchall()
                for group in groups:
                    self._start_rebalance(connection, group["group_id"])
        return NodeRecord(node_id, role, host, port, capacity, "STARTING", now)

    def heartbeat(self, node_id: str) -> NodeRecord:
        now = int(time.time() * 1000)
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE nodes SET status = 'HEALTHY', last_heartbeat_ms = ?
                WHERE node_id = ?
                """,
                (now, node_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"Unknown node {node_id!r}")
            connection.execute(
                "UPDATE node_health SET status = 'HEALTHY' WHERE node_id = ?", (node_id,)
            )
            row = connection.execute(
                """
                SELECT n.*, h.status AS health_status FROM nodes AS n
                JOIN node_health AS h ON h.node_id = n.node_id
                WHERE n.node_id = ?
                """,
                (node_id,),
            ).fetchone()
        return self._node_from_row(row)

    def list_nodes(self, role: str | None = None) -> list[NodeRecord]:
        with self._connect() as connection:
            if role is None:
                rows = connection.execute(
                    """
                    SELECT n.*, h.status AS health_status FROM nodes AS n
                    JOIN node_health AS h ON h.node_id = n.node_id ORDER BY n.node_id
                    """
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT n.*, h.status AS health_status FROM nodes AS n
                    JOIN node_health AS h ON h.node_id = n.node_id
                    WHERE n.role = ? ORDER BY n.node_id
                    """,
                    (role,),
                ).fetchall()
        return [self._node_from_row(row) for row in rows]

    def create_topic(self, topic: str, partition_count: int) -> list[dict[str, object]]:
        if not topic or len(topic) > 128:
            raise ValueError("topic must contain 1 to 128 characters")
        if isinstance(partition_count, bool) or not isinstance(partition_count, int):
            raise ValueError("partition_count must be an integer")
        if partition_count <= 0 or partition_count > 100_000:
            raise ValueError("partition_count must be between 1 and 100000")

        with self._connect() as connection:
            brokers = connection.execute(
                """
                SELECT n.* FROM nodes AS n
                JOIN node_health AS h ON h.node_id = n.node_id
                WHERE n.role = 'broker' AND h.status = 'HEALTHY'
                ORDER BY n.node_id
                """
            ).fetchall()
            if not brokers:
                raise ValueError("Cannot create a topic without a healthy broker")
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT partition_count FROM topics WHERE name = ?", (topic,)
            ).fetchone()
            if existing:
                if existing["partition_count"] != partition_count:
                    raise ValueError(
                        f"Topic {topic!r} already exists with "
                        f"{existing['partition_count']} partitions"
                    )
            else:
                connection.execute(
                    "INSERT INTO topics(name, partition_count, created_ms) VALUES (?, ?, ?)",
                    (topic, partition_count, int(time.time() * 1000)),
                )
                for partition_id in range(partition_count):
                    broker = brokers[partition_id % len(brokers)]
                    connection.execute(
                        """
                        INSERT INTO partitions(topic, partition_id, broker_id)
                        VALUES (?, ?, ?)
                        """,
                        (topic, partition_id, broker["node_id"]),
                    )
            return self._topic_partitions(connection, topic)

    def get_topic(self, topic: str) -> list[dict[str, object]]:
        with self._connect() as connection:
            if (
                connection.execute("SELECT 1 FROM topics WHERE name = ?", (topic,)).fetchone()
                is None
            ):
                raise KeyError(f"Unknown topic {topic!r}")
            return self._topic_partitions(connection, topic)

    def get_partition_route(self, topic: str, partition_id: int) -> dict[str, object]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT p.topic, p.partition_id, p.broker_id,
                       n.host AS broker_host, n.port AS broker_port, t.partition_count
                FROM partitions AS p
                JOIN topics AS t ON t.name = p.topic
                JOIN nodes AS n ON n.node_id = p.broker_id
                WHERE p.topic = ? AND p.partition_id = ?
                """,
                (topic, partition_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"Unknown partition {topic}/{partition_id}")
            if row["broker_id"] is None or row["broker_host"] is None:
                raise ValueError(f"Partition {topic}/{partition_id} has no broker route")
            health = connection.execute(
                "SELECT status FROM node_health WHERE node_id = ?", (row["broker_id"],)
            ).fetchone()
            if health is None or health["status"] != "HEALTHY":
                raise ValueError(f"Partition {topic}/{partition_id} broker is not healthy")
            return dict(row)

    def report_metrics(
        self,
        node_id: str,
        instance_id: str,
        sample_interval_ms: int,
        payload: dict[str, object],
        *,
        config: PressureConfig = PressureConfig(),
    ) -> dict[str, object]:
        if not node_id or not instance_id or len(instance_id) > 128:
            raise ValueError("node_id and instance_id must be non-empty and bounded")
        if (
            isinstance(sample_interval_ms, bool)
            or not isinstance(sample_interval_ms, int)
            or sample_interval_ms < 0
        ):
            raise ValueError("sample_interval_ms must be a non-negative integer")
        metrics = validate_metrics(payload)
        node_time = int(time.time() * 1000)
        partition_lags = payload.get("partition_lags", [])
        if not isinstance(partition_lags, list):
            raise ValueError("partition_lags must be a list")
        lag_total = 0
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            node = connection.execute(
                """
                SELECT n.role, h.status FROM nodes AS n
                JOIN node_health AS h ON h.node_id = n.node_id WHERE n.node_id = ?
                """,
                (node_id,),
            ).fetchone()
            if node is None or node["status"] != "HEALTHY":
                raise ValueError(f"Node {node_id!r} is not healthy")
            if node["role"] != "worker" and partition_lags:
                raise ValueError("Only workers may report consumer partition lag")

            validated_lags: list[tuple[str, str, int, int]] = []
            for item in partition_lags:
                if not isinstance(item, dict):
                    raise ValueError("each partition lag entry must be an object")
                group_id = item.get("group_id")
                topic = item.get("topic")
                partition_id = item.get("partition_id")
                lag_records = item.get("lag_records")
                if (
                    not isinstance(group_id, str)
                    or not group_id
                    or not isinstance(topic, str)
                    or not topic
                    or isinstance(partition_id, bool)
                    or not isinstance(partition_id, int)
                    or partition_id < 0
                    or isinstance(lag_records, bool)
                    or not isinstance(lag_records, int)
                    or lag_records < 0
                ):
                    raise ValueError("partition lag has invalid group/topic/partition/lag fields")
                assignment = connection.execute(
                    """
                    SELECT 1 FROM assignments
                    WHERE group_id = ? AND topic = ? AND partition_id = ?
                      AND worker_id = ? AND state IN ('ACTIVE', 'REVOKING')
                    """,
                    (group_id, topic, partition_id, node_id),
                ).fetchone()
                if assignment is None:
                    raise ValueError("partition lag must refer to a current worker assignment")
                validated_lags.append((group_id, topic, partition_id, lag_records))
                lag_total += lag_records

            components, pressure = normalized_pressure(
                cpu_percent=float(metrics["cpu_percent"]),
                queue_depth=int(metrics["queue_depth"]),
                queue_capacity=int(metrics["queue_capacity"]),
                p95_latency_ms=float(metrics["p95_latency_ms"]),
                memory_percent=float(metrics["memory_percent"]),
                lag_records=lag_total,
            )
            previous = connection.execute(
                "SELECT * FROM node_metrics WHERE node_id = ?", (node_id,)
            ).fetchone()
            previous_pressure = (
                previous["ema_pressure"]
                if previous is not None and previous["instance_id"] == instance_id
                else None
            )
            ema_pressure = smooth_pressure(previous_pressure, pressure, config.alpha)
            was_overloaded = bool(previous["overloaded"]) if previous is not None else False
            if was_overloaded:
                overloaded = ema_pressure >= config.overloaded_exit
            else:
                overloaded = ema_pressure >= config.overloaded_enter
            component_json = json.dumps(components, sort_keys=True, separators=(",", ":"))
            values = (
                node_id,
                instance_id,
                node_time,
                sample_interval_ms,
                metrics["cpu_percent"],
                metrics["memory_percent"],
                metrics["queue_depth"],
                metrics["queue_capacity"],
                metrics["p95_latency_ms"],
                metrics["processing_rate"],
                metrics["in_flight"],
                lag_total,
                pressure,
                ema_pressure,
                int(overloaded),
                component_json,
            )
            connection.execute(
                """
                INSERT INTO node_metrics(
                    node_id, instance_id, timestamp_ms, sample_interval_ms,
                    cpu_percent, memory_percent, queue_depth, queue_capacity,
                    p95_latency_ms, processing_rate, in_flight, lag_records,
                    raw_pressure, ema_pressure, overloaded, components_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    instance_id = excluded.instance_id,
                    timestamp_ms = excluded.timestamp_ms,
                    sample_interval_ms = excluded.sample_interval_ms,
                    cpu_percent = excluded.cpu_percent,
                    memory_percent = excluded.memory_percent,
                    queue_depth = excluded.queue_depth,
                    queue_capacity = excluded.queue_capacity,
                    p95_latency_ms = excluded.p95_latency_ms,
                    processing_rate = excluded.processing_rate,
                    in_flight = excluded.in_flight,
                    lag_records = excluded.lag_records,
                    raw_pressure = excluded.raw_pressure,
                    ema_pressure = excluded.ema_pressure,
                    overloaded = excluded.overloaded,
                    components_json = excluded.components_json
                """,
                values,
            )
            connection.execute(
                """
                INSERT INTO metric_history(
                    node_id, instance_id, timestamp_ms, sample_interval_ms,
                    cpu_percent, memory_percent, queue_depth, queue_capacity,
                    p95_latency_ms, processing_rate, in_flight, lag_records,
                    raw_pressure, ema_pressure, overloaded, components_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            connection.execute("DELETE FROM partition_metrics WHERE worker_id = ?", (node_id,))
            for group_id, topic, partition_id, lag_records in validated_lags:
                connection.execute(
                    """
                    INSERT INTO partition_metrics(
                        group_id, topic, partition_id, worker_id, lag_records, timestamp_ms
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (group_id, topic, partition_id, node_id, lag_records, node_time),
                )
            self._verify_moved_decisions(connection, node_time)
            return {
                "node_id": node_id,
                "timestamp_ms": node_time,
                "components": components,
                "raw_pressure": pressure,
                "ema_pressure": ema_pressure,
                "overloaded": overloaded,
                "lag_records": lag_total,
            }

    def optimize_group(
        self,
        group_id: str,
        *,
        config: PressureConfig = PressureConfig(),
        now_ms: int | None = None,
    ) -> dict[str, object]:
        if not group_id or len(group_id) > 128:
            raise ValueError("group_id must contain 1 to 128 characters")
        now = int(time.time() * 1000) if now_ms is None else now_ms
        if isinstance(now, bool) or not isinstance(now, int) or now < 0:
            raise ValueError("now_ms must be a non-negative integer")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            busy = connection.execute(
                """
                SELECT decision_id FROM optimizer_decisions
                WHERE group_id = ? AND status IN ('PENDING', 'MOVED')
                ORDER BY created_ms DESC LIMIT 1
                """,
                (group_id,),
            ).fetchone()
            if busy is not None:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "A previous partition move is awaiting revocation or verification.",
                    now,
                    details={"pending_decision_id": busy["decision_id"]},
                )
            worker_move = connection.execute(
                """
                SELECT decision_id, source_worker, destination_worker
                FROM optimizer_moves WHERE state = 'PENDING'
                  AND (source_worker IN (
                      SELECT worker_id FROM group_members WHERE group_id = ?
                  ) OR destination_worker IN (
                      SELECT worker_id FROM group_members WHERE group_id = ?
                  ))
                LIMIT 1
                """,
                (group_id, group_id),
            ).fetchone()
            if worker_move is not None:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "A worker involved in another partition move is still draining.",
                    now,
                    details={"pending_decision_id": worker_move["decision_id"]},
                )

            members = connection.execute(
                """
                SELECT n.node_id, n.capacity, m.ema_pressure, m.raw_pressure,
                       m.overloaded, m.timestamp_ms, m.cpu_percent, m.memory_percent,
                       m.queue_depth, m.queue_capacity, m.p95_latency_ms, m.lag_records,
                       m.components_json
                FROM group_members AS gm
                JOIN nodes AS n ON n.node_id = gm.worker_id
                JOIN node_health AS h ON h.node_id = n.node_id
                JOIN node_metrics AS m ON m.node_id = n.node_id
                WHERE gm.group_id = ? AND h.status = 'HEALTHY' AND m.timestamp_ms >= ?
                ORDER BY n.node_id
                """,
                (group_id, now - int(config.max_sample_age_seconds * 1000)),
            ).fetchall()
            if len(members) < 2:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "At least two healthy workers with fresh metrics are required.",
                    now,
                )
            sources = [row for row in members if row["overloaded"]]
            if not sources:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "No worker crossed the overload hysteresis enter threshold.",
                    now,
                    details={
                        "threshold": config.overloaded_enter,
                        "pressures": {row["node_id"]: row["ema_pressure"] for row in members},
                    },
                )
            sources.sort(key=lambda row: (-row["ema_pressure"], row["node_id"]))
            source = sources[0]
            destinations = [
                row
                for row in members
                if row["node_id"] != source["node_id"]
                and row["ema_pressure"] < config.overloaded_exit
            ]
            if not destinations:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "No healthy destination is below the overload exit threshold.",
                    now,
                    details={"source": source["node_id"]},
                )
            destinations.sort(key=lambda row: (row["ema_pressure"], row["node_id"]))
            destination = destinations[0]

            cooldown_ms = int(config.cooldown_seconds * 1000)
            last_move = connection.execute(
                """
                SELECT created_ms FROM optimizer_decisions
                WHERE group_id = ? AND decision_type = 'MOVE_PARTITION'
                  AND status IN ('MOVED', 'VERIFIED')
                ORDER BY created_ms DESC LIMIT 1
                """,
                (group_id,),
            ).fetchone()
            if last_move is not None and now - last_move["created_ms"] < cooldown_ms:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "Move cooldown is active.",
                    now,
                    details={"remaining_ms": cooldown_ms - (now - last_move["created_ms"])},
                )
            source_move = connection.execute(
                """
                SELECT d.created_ms FROM optimizer_decisions AS d
                WHERE d.source_worker = ? AND d.decision_type = 'MOVE_PARTITION'
                  AND d.status IN ('MOVED', 'VERIFIED')
                ORDER BY d.created_ms DESC LIMIT 1
                """,
                (source["node_id"],),
            ).fetchone()
            if source_move is not None and now - source_move["created_ms"] < cooldown_ms:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "Source worker migration cooldown is active.",
                    now,
                    details={"remaining_ms": cooldown_ms - (now - source_move["created_ms"])},
                )
            budget_count = connection.execute(
                """
                SELECT COUNT(*) AS count FROM optimizer_decisions
                WHERE group_id = ? AND decision_type = 'MOVE_PARTITION'
                  AND created_ms >= ?
                """,
                (group_id, now - int(config.budget_window_seconds * 1000)),
            ).fetchone()["count"]
            if budget_count >= config.migration_budget:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "Migration budget for the configured window is exhausted.",
                    now,
                    details={
                        "budget": config.migration_budget,
                        "window_seconds": config.budget_window_seconds,
                    },
                )
            source_budget_count = connection.execute(
                """
                SELECT COUNT(*) AS count FROM optimizer_decisions
                WHERE source_worker = ? AND decision_type = 'MOVE_PARTITION'
                  AND created_ms >= ?
                """,
                (source["node_id"], now - int(config.budget_window_seconds * 1000)),
            ).fetchone()["count"]
            if source_budget_count >= config.migration_budget:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "Source worker migration budget for the configured window is exhausted.",
                    now,
                    details={"budget": config.migration_budget},
                )

            if destination["capacity"] < source["capacity"] * 0.5:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "Destination has less than half the source worker's configured capacity.",
                    now,
                    details={"source": source["node_id"], "destination": destination["node_id"]},
                )

            partition = connection.execute(
                """
                SELECT a.topic, a.partition_id, a.epoch,
                       COALESCE(pm.lag_records, 0) AS lag_records
                FROM assignments AS a
                LEFT JOIN partition_metrics AS pm
                  ON pm.group_id = a.group_id AND pm.topic = a.topic
                 AND pm.partition_id = a.partition_id AND pm.worker_id = a.worker_id
                WHERE a.group_id = ? AND a.worker_id = ? AND a.state = 'ACTIVE'
                ORDER BY lag_records DESC, a.topic, a.partition_id
                LIMIT 1
                """,
                (group_id, source["node_id"]),
            ).fetchone()
            if partition is None:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "Overloaded worker has no active partition eligible to move.",
                    now,
                    details={"source": source["node_id"]},
                )
            benefit = source["ema_pressure"] - destination["ema_pressure"] - config.migration_cost
            if benefit < config.minimum_benefit:
                return self._record_decision(
                    connection,
                    group_id,
                    "NO_ACTION",
                    "Estimated benefit does not exceed migration cost and minimum benefit.",
                    now,
                    details={
                        "source_pressure": source["ema_pressure"],
                        "destination_pressure": destination["ema_pressure"],
                        "estimated_benefit": benefit,
                    },
                )

            details = {
                "source_pressure": source["ema_pressure"],
                "destination_pressure": destination["ema_pressure"],
                "raw_source_pressure": source["raw_pressure"],
                "raw_destination_pressure": destination["raw_pressure"],
                "source_metrics": json.loads(source["components_json"]),
                "destination_metrics": json.loads(destination["components_json"]),
                "partition_lag": partition["lag_records"],
                "migration_cost": config.migration_cost,
                "minimum_benefit": config.minimum_benefit,
                "predicted_benefit": benefit,
            }
            decision = self._record_decision(
                connection,
                group_id,
                "MOVE_PARTITION",
                (
                    f"Worker {source['node_id']} pressure {source['ema_pressure']:.3f} "
                    f"exceeds the hysteresis threshold; {destination['node_id']} is "
                    f"at {destination['ema_pressure']:.3f}. Moving the highest-lag "
                    f"eligible partition has predicted net benefit {benefit:.3f}."
                ),
                now,
                details=details,
                source_worker=source["node_id"],
                destination_worker=destination["node_id"],
                topic=partition["topic"],
                partition_id=partition["partition_id"],
                predicted_benefit=benefit,
                source_pressure=source["ema_pressure"],
                destination_pressure=destination["ema_pressure"],
            )
            connection.execute(
                """
                INSERT INTO optimizer_moves(
                    decision_id, group_id, topic, partition_id, source_worker,
                    destination_worker, state
                ) VALUES (?, ?, ?, ?, ?, ?, 'PENDING')
                """,
                (
                    decision["decision_id"],
                    group_id,
                    partition["topic"],
                    partition["partition_id"],
                    source["node_id"],
                    destination["node_id"],
                ),
            )
            self._start_rebalance(
                connection,
                group_id,
                topic=partition["topic"],
                partition_id=partition["partition_id"],
            )
            return decision

    def get_decisions(self, group_id: str, *, limit: int = 100) -> list[dict[str, object]]:
        if not group_id or len(group_id) > 128:
            raise ValueError("group_id must contain 1 to 128 characters")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise ValueError("limit must be between 1 and 1000")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM optimizer_decisions
                WHERE group_id = ? ORDER BY created_ms DESC LIMIT ?
                """,
                (group_id, limit),
            ).fetchall()
        return [self._decision_from_row(row) for row in rows]

    def get_metrics(self, node_id: str | None = None) -> list[dict[str, object]]:
        with self._connect() as connection:
            if node_id is None:
                rows = connection.execute(
                    """
                    SELECT m.*, n.role, h.status AS node_status FROM node_metrics AS m
                    JOIN nodes AS n ON n.node_id = m.node_id
                    JOIN node_health AS h ON h.node_id = m.node_id
                    ORDER BY m.node_id
                    """
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT m.*, n.role, h.status AS node_status FROM node_metrics AS m
                    JOIN nodes AS n ON n.node_id = m.node_id
                    JOIN node_health AS h ON h.node_id = m.node_id
                    WHERE m.node_id = ?
                    """,
                    (node_id,),
                ).fetchall()
        return [
            {
                **dict(row),
                "components": json.loads(row["components_json"]),
                "overloaded": bool(row["overloaded"]),
            }
            for row in rows
        ]

    @staticmethod
    def _record_decision(
        connection: sqlite3.Connection,
        group_id: str,
        decision_type: str,
        reason: str,
        now_ms: int,
        *,
        details: dict[str, object] | None = None,
        source_worker: str | None = None,
        destination_worker: str | None = None,
        topic: str | None = None,
        partition_id: int | None = None,
        predicted_benefit: float | None = None,
        source_pressure: float | None = None,
        destination_pressure: float | None = None,
    ) -> dict[str, object]:
        decision_id = str(uuid4())
        connection.execute(
            """
            INSERT INTO optimizer_decisions(
                decision_id, group_id, decision_type, status, reason, created_ms,
                source_worker, destination_worker, topic, partition_id,
                predicted_benefit, source_pressure_before, destination_pressure_before,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_id,
                group_id,
                decision_type,
                "PENDING" if decision_type == "MOVE_PARTITION" else "NO_ACTION",
                reason,
                now_ms,
                source_worker,
                destination_worker,
                topic,
                partition_id,
                predicted_benefit,
                source_pressure,
                destination_pressure,
                json.dumps(details or {}, sort_keys=True, separators=(",", ":")),
            ),
        )
        return {
            "decision_id": decision_id,
            "group_id": group_id,
            "decision_type": decision_type,
            "status": "PENDING" if decision_type == "MOVE_PARTITION" else "NO_ACTION",
            "reason": reason,
            "created_ms": now_ms,
            "source_worker": source_worker,
            "destination_worker": destination_worker,
            "topic": topic,
            "partition_id": partition_id,
            "predicted_benefit": predicted_benefit,
            "actual_benefit": None,
            "details": details or {},
        }

    @staticmethod
    def _decision_from_row(row: sqlite3.Row) -> dict[str, object]:
        return {
            "decision_id": row["decision_id"],
            "group_id": row["group_id"],
            "decision_type": row["decision_type"],
            "status": row["status"],
            "reason": row["reason"],
            "created_ms": row["created_ms"],
            "source_worker": row["source_worker"],
            "destination_worker": row["destination_worker"],
            "topic": row["topic"],
            "partition_id": row["partition_id"],
            "predicted_benefit": row["predicted_benefit"],
            "actual_benefit": row["actual_benefit"],
            "prediction_error": row["prediction_error"],
            "details": json.loads(row["details_json"]),
        }

    @staticmethod
    def _verify_moved_decisions(connection: sqlite3.Connection, now_ms: int) -> None:
        moved = connection.execute(
            """
            SELECT d.*, s.ema_pressure AS source_pressure_after,
                   s.timestamp_ms AS source_timestamp,
                   t.ema_pressure AS target_pressure_after,
                   t.timestamp_ms AS target_timestamp
            FROM optimizer_decisions AS d
            JOIN node_metrics AS s ON s.node_id = d.source_worker
            JOIN node_metrics AS t ON t.node_id = d.destination_worker
            WHERE d.status = 'MOVED'
            """
        ).fetchall()
        for decision in moved:
            if (
                decision["source_timestamp"] <= decision["moved_ms"]
                or decision["target_timestamp"] <= decision["moved_ms"]
            ):
                continue
            before = (
                decision["source_pressure_before"] + decision["destination_pressure_before"]
            ) / 2
            after = (decision["source_pressure_after"] + decision["target_pressure_after"]) / 2
            actual = before - after
            connection.execute(
                """
                UPDATE optimizer_decisions SET status = 'VERIFIED', verified_ms = ?,
                    actual_benefit = ?, prediction_error = ? WHERE decision_id = ?
                """,
                (
                    now_ms,
                    actual,
                    decision["predicted_benefit"] - actual,
                    decision["decision_id"],
                ),
            )

    def join_group(
        self,
        group_id: str,
        worker_id: str,
        topics: list[str],
        *,
        capacity: float = 1.0,
    ) -> list[PartitionAssignment]:
        if not group_id or len(group_id) > 128:
            raise ValueError("group_id must contain 1 to 128 characters")
        if not worker_id or len(worker_id) > 128:
            raise ValueError("worker_id must contain 1 to 128 characters")
        if not topics or any(not isinstance(topic, str) or not topic for topic in topics):
            raise ValueError("topics must be a non-empty list of topic names")
        if (
            isinstance(capacity, bool)
            or not isinstance(capacity, (int, float))
            or not isfinite(capacity)
            or capacity <= 0
        ):
            raise ValueError("capacity must be positive")

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            worker = connection.execute(
                "SELECT * FROM nodes WHERE node_id = ? AND role = 'worker'",
                (worker_id,),
            ).fetchone()
            health = (
                connection.execute(
                    "SELECT status FROM node_health WHERE node_id = ?", (worker_id,)
                ).fetchone()
                if worker is not None
                else None
            )
            if worker is None or health is None or health["status"] != "HEALTHY":
                raise ValueError(f"Worker {worker_id!r} is not registered and healthy")
            changed = float(worker["capacity"]) != float(capacity)
            connection.execute(
                "UPDATE nodes SET capacity = ? WHERE node_id = ?", (capacity, worker_id)
            )

            for topic in topics:
                if (
                    connection.execute("SELECT 1 FROM topics WHERE name = ?", (topic,)).fetchone()
                    is None
                ):
                    raise KeyError(f"Unknown topic {topic!r}")
                topic_cursor = connection.execute(
                    "INSERT OR IGNORE INTO group_topics(group_id, topic) VALUES (?, ?)",
                    (group_id, topic),
                )
                changed = changed or topic_cursor.rowcount > 0
            member_cursor = connection.execute(
                "INSERT OR IGNORE INTO group_members(group_id, worker_id) VALUES (?, ?)",
                (group_id, worker_id),
            )
            changed = changed or member_cursor.rowcount > 0
            if changed:
                self._start_rebalance(connection, group_id)
                has_assignments = connection.execute(
                    "SELECT 1 FROM assignments WHERE group_id = ? LIMIT 1", (group_id,)
                ).fetchone()
                if has_assignments is None:
                    self._finalize_rebalance(connection, group_id)
            self._maybe_finalize_rebalance(connection, group_id)
            return self._get_assignments(connection, group_id, worker_id)

    def acknowledge_revocation(
        self, group_id: str, worker_id: str, topic: str, partition_id: int, epoch: int
    ) -> list[PartitionAssignment]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE assignments SET revoke_acked = 1
                WHERE group_id = ? AND worker_id = ? AND topic = ? AND partition_id = ?
                  AND epoch = ? AND state = 'REVOKING'
                """,
                (group_id, worker_id, topic, partition_id, epoch),
            )
            if cursor.rowcount != 1:
                current = connection.execute(
                    """
                    SELECT epoch FROM assignments
                    WHERE group_id = ? AND topic = ? AND partition_id = ?
                    """,
                    (group_id, topic, partition_id),
                ).fetchone()
                if current is None or current["epoch"] <= epoch:
                    raise AssignmentValidationError(
                        "STALE_EPOCH", "Revocation does not match the current assignment"
                    )
                return self._get_assignments(connection, group_id, worker_id)
            self._maybe_finalize_rebalance(connection, group_id)
            return self._get_assignments(connection, group_id, worker_id)

    def leave_group(self, group_id: str, worker_id: str) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            member = connection.execute(
                "DELETE FROM group_members WHERE group_id = ? AND worker_id = ?",
                (group_id, worker_id),
            )
            if member.rowcount == 0:
                return
            self._start_rebalance(connection, group_id)
            connection.execute(
                """
                UPDATE assignments SET revoke_acked = 1
                WHERE group_id = ? AND worker_id = ? AND state = 'REVOKING'
                """,
                (group_id, worker_id),
            )
            self._maybe_finalize_rebalance(connection, group_id)

    def validate_assignment(
        self,
        group_id: str,
        topic: str,
        partition_id: int,
        worker_id: str,
        broker_id: str,
        epoch: int,
        operation: str,
    ) -> None:
        if operation not in {"FETCH", "COMMIT_OFFSET"}:
            raise ValueError("Unsupported assignment operation")
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT a.worker_id, a.epoch, a.state, p.broker_id, h.status,
                       broker_health.status AS broker_status
                FROM assignments AS a
                JOIN partitions AS p
                  ON p.topic = a.topic AND p.partition_id = a.partition_id
                JOIN nodes AS n ON n.node_id = a.worker_id
                JOIN node_health AS h ON h.node_id = n.node_id
                JOIN node_health AS broker_health ON broker_health.node_id = p.broker_id
                WHERE a.group_id = ? AND a.topic = ? AND a.partition_id = ?
                """,
                (group_id, topic, partition_id),
            ).fetchone()
            if (
                row is None
                or row["worker_id"] != worker_id
                or row["broker_id"] != broker_id
                or row["epoch"] != epoch
                or row["state"] == "UNASSIGNED"
            ):
                raise AssignmentValidationError(
                    "STALE_EPOCH", "Worker does not own this partition at the supplied epoch"
                )
            if row["broker_status"] != "HEALTHY":
                raise AssignmentValidationError(
                    "BROKER_NOT_HEALTHY", "Partition broker is not healthy"
                )
            if row["status"] == "UNAVAILABLE":
                raise AssignmentValidationError(
                    "STALE_EPOCH", "Worker is unavailable for this assignment"
                )
            if row["state"] == "REVOKING" and operation == "FETCH":
                raise AssignmentValidationError(
                    "ASSIGNMENT_REVOKING", "Partition is being safely revoked"
                )
            if row["state"] == "ACTIVE" and row["status"] != "HEALTHY":
                raise AssignmentValidationError(
                    "STALE_EPOCH", "Worker is not healthy for this assignment"
                )

    def expire_nodes(
        self,
        *,
        now_ms: int | None = None,
        heartbeat_timeout_ms: int,
        suspect_grace_ms: int,
    ) -> list[NodeRecord]:
        if (
            isinstance(heartbeat_timeout_ms, bool)
            or not isinstance(heartbeat_timeout_ms, int)
            or heartbeat_timeout_ms <= 0
            or isinstance(suspect_grace_ms, bool)
            or not isinstance(suspect_grace_ms, int)
            or suspect_grace_ms <= 0
        ):
            raise ValueError("heartbeat timeout and suspect grace must be positive")
        if now_ms is not None and (
            isinstance(now_ms, bool) or not isinstance(now_ms, int) or now_ms < 0
        ):
            raise ValueError("now_ms must be a non-negative integer")
        now = int(time.time() * 1000) if now_ms is None else now_ms
        changed: list[NodeRecord] = []
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            nodes = connection.execute(
                """
                SELECT n.*, h.status AS health_status FROM nodes AS n
                JOIN node_health AS h ON h.node_id = n.node_id
                WHERE h.status != 'UNAVAILABLE'
                """,
            ).fetchall()
            groups_to_rebalance: set[str] = set()
            for node in nodes:
                age = now - node["last_heartbeat_ms"]
                status = node["health_status"]
                if status in {"HEALTHY", "STARTING"} and age >= heartbeat_timeout_ms:
                    next_status = "SUSPECTED"
                elif status == "SUSPECTED" and age >= heartbeat_timeout_ms + suspect_grace_ms:
                    next_status = "UNAVAILABLE"
                else:
                    continue
                connection.execute(
                    "UPDATE node_health SET status = ? WHERE node_id = ?",
                    (next_status, node["node_id"]),
                )
                if node["role"] == "worker":
                    groups = connection.execute(
                        "SELECT group_id FROM group_members WHERE worker_id = ?",
                        (node["node_id"],),
                    ).fetchall()
                    for group in groups:
                        group_id = group["group_id"]
                        groups_to_rebalance.add(group_id)
                        self._start_rebalance(connection, group_id)
                    if next_status == "UNAVAILABLE":
                        connection.execute(
                            "DELETE FROM group_members WHERE worker_id = ?", (node["node_id"],)
                        )
                        connection.execute(
                            """
                            UPDATE assignments SET revoke_acked = 1
                            WHERE worker_id = ? AND state = 'REVOKING'
                            """,
                            (node["node_id"],),
                        )
                row = connection.execute(
                    """
                    SELECT n.*, h.status AS health_status FROM nodes AS n
                    JOIN node_health AS h ON h.node_id = n.node_id WHERE n.node_id = ?
                    """,
                    (node["node_id"],),
                ).fetchone()
                changed.append(self._node_from_row(row))
            for group_id in groups_to_rebalance:
                self._maybe_finalize_rebalance(connection, group_id)
        return changed

    def _start_rebalance(
        self,
        connection: sqlite3.Connection,
        group_id: str,
        *,
        topic: str | None = None,
        partition_id: int | None = None,
    ) -> None:
        if topic is None:
            connection.execute(
                """
                UPDATE assignments SET state = 'REVOKING', revoke_acked = 0
                WHERE group_id = ? AND state IN ('ACTIVE', 'UNASSIGNED')
                """,
                (group_id,),
            )
        else:
            connection.execute(
                """
                UPDATE assignments SET state = 'REVOKING', revoke_acked = 0
                WHERE group_id = ? AND topic = ? AND partition_id = ?
                  AND state = 'ACTIVE'
                """,
                (group_id, topic, partition_id),
            )
        connection.execute(
            """
            UPDATE assignments SET revoke_acked = 1
            WHERE group_id = ? AND state = 'REVOKING'
              AND worker_id IN (
                  SELECT n.node_id FROM nodes AS n
                  JOIN node_health AS h ON h.node_id = n.node_id
                  WHERE h.status = 'UNAVAILABLE'
              )
            """,
            (group_id,),
        )

    def _maybe_finalize_rebalance(self, connection: sqlite3.Connection, group_id: str) -> None:
        pending = connection.execute(
            """
            SELECT 1 FROM assignments AS a
            JOIN node_health AS h ON h.node_id = a.worker_id
            WHERE a.group_id = ? AND a.state = 'REVOKING'
              AND a.revoke_acked = 0 AND h.status != 'UNAVAILABLE'
            LIMIT 1
            """,
            (group_id,),
        ).fetchone()
        revoking = connection.execute(
            "SELECT 1 FROM assignments WHERE group_id = ? AND state = 'REVOKING' LIMIT 1",
            (group_id,),
        ).fetchone()
        if pending is None and revoking is not None:
            self._finalize_rebalance(connection, group_id)

    def _finalize_rebalance(self, connection: sqlite3.Connection, group_id: str) -> None:
        members = connection.execute(
            """
            SELECT n.node_id, n.capacity
            FROM group_members AS gm
            JOIN nodes AS n ON n.node_id = gm.worker_id
            JOIN node_health AS h ON h.node_id = n.node_id
            WHERE gm.group_id = ? AND h.status = 'HEALTHY'
            ORDER BY n.node_id
            """,
            (group_id,),
        ).fetchall()
        partitions = connection.execute(
            """
            SELECT p.topic, p.partition_id, p.broker_id, n.host, n.port
            FROM group_topics AS gt
            JOIN partitions AS p ON p.topic = gt.topic
            JOIN nodes AS n ON n.node_id = p.broker_id
            JOIN node_health AS h ON h.node_id = n.node_id
            WHERE gt.group_id = ? AND h.status = 'HEALTHY'
            ORDER BY p.topic, p.partition_id
            """,
            (group_id,),
        ).fetchall()
        existing = {
            (row["topic"], row["partition_id"]): row
            for row in connection.execute(
                "SELECT * FROM assignments WHERE group_id = ?", (group_id,)
            ).fetchall()
        }
        pending_move = connection.execute(
            """
            SELECT * FROM optimizer_moves WHERE group_id = ? AND state = 'PENDING'
            LIMIT 1
            """,
            (group_id,),
        ).fetchone()
        if not members:
            for previous in existing.values():
                connection.execute(
                    """
                    UPDATE assignments SET epoch = epoch + 1, state = 'UNASSIGNED',
                                           revoke_acked = 0
                    WHERE group_id = ? AND topic = ? AND partition_id = ?
                    """,
                    (group_id, previous["topic"], previous["partition_id"]),
                )
            if pending_move is not None:
                connection.execute(
                    """
                    UPDATE optimizer_moves SET state = 'FAILED' WHERE decision_id = ?
                    """,
                    (pending_move["decision_id"],),
                )
                connection.execute(
                    """
                    UPDATE optimizer_decisions SET status = 'FAILED', verified_ms = ?,
                        reason = reason || ' No healthy worker remained to complete the move.'
                    WHERE decision_id = ?
                    """,
                    (int(time.time() * 1000), pending_move["decision_id"]),
                )
            return

        loads = {member["node_id"]: 0.0 for member in members}
        capacities = {member["node_id"]: float(member["capacity"]) for member in members}
        member_ids = set(loads)
        if pending_move is not None and (
            pending_move["destination_worker"] not in member_ids
            or not any(
                partition["topic"] == pending_move["topic"]
                and partition["partition_id"] == pending_move["partition_id"]
                for partition in partitions
            )
        ):
            connection.execute(
                "UPDATE optimizer_moves SET state = 'FAILED' WHERE decision_id = ?",
                (pending_move["decision_id"],),
            )
            connection.execute(
                """
                UPDATE optimizer_decisions SET status = 'FAILED', verified_ms = ?,
                    reason = reason || ' Target became unavailable before revocation completed.'
                WHERE decision_id = ?
                """,
                (int(time.time() * 1000), pending_move["decision_id"]),
            )
            pending_move = None
        if pending_move is not None:
            for partition in partitions:
                key = (partition["topic"], partition["partition_id"])
                previous = existing.get(key)
                is_target = key == (pending_move["topic"], pending_move["partition_id"])
                if is_target:
                    owner = pending_move["destination_worker"]
                elif previous is not None and previous["worker_id"] in member_ids:
                    owner = previous["worker_id"]
                else:
                    owner = min(loads, key=lambda item: (loads[item] / capacities[item], item))
                self._save_assignment(connection, group_id, partition, previous, owner)
                loads[owner] += 1.0
            connection.execute(
                "UPDATE optimizer_moves SET state = 'MOVED' WHERE decision_id = ?",
                (pending_move["decision_id"],),
            )
            connection.execute(
                """
                UPDATE optimizer_decisions SET status = 'MOVED', moved_ms = ?
                WHERE decision_id = ?
                """,
                (int(time.time() * 1000), pending_move["decision_id"]),
            )
            return

        assigned: set[tuple[str, int]] = set()
        for partition in partitions:
            owner = min(loads, key=lambda item: (loads[item] / capacities[item], item))
            key = (partition["topic"], partition["partition_id"])
            assigned.add(key)
            previous = existing.get(key)
            self._save_assignment(connection, group_id, partition, previous, owner)
            loads[owner] += 1.0
        for key, previous in existing.items():
            if key not in assigned:
                connection.execute(
                    """
                    UPDATE assignments SET epoch = epoch + 1, state = 'UNASSIGNED',
                                           revoke_acked = 0
                    WHERE group_id = ? AND topic = ? AND partition_id = ?
                    """,
                    (group_id, previous["topic"], previous["partition_id"]),
                )

    @staticmethod
    def _save_assignment(
        connection: sqlite3.Connection,
        group_id: str,
        partition: sqlite3.Row,
        previous: sqlite3.Row | None,
        owner: str,
    ) -> None:
        key = (partition["topic"], partition["partition_id"])
        epoch = (
            previous["epoch"]
            if previous is not None and previous["worker_id"] == owner
            else (previous["epoch"] + 1 if previous is not None else 1)
        )
        connection.execute(
            """
            INSERT INTO assignments(
                group_id, topic, partition_id, worker_id, epoch, state, revoke_acked
            ) VALUES (?, ?, ?, ?, ?, 'ACTIVE', 0)
            ON CONFLICT(group_id, topic, partition_id) DO UPDATE SET
                worker_id = excluded.worker_id,
                epoch = excluded.epoch,
                state = 'ACTIVE',
                revoke_acked = 0
            """,
            (group_id, key[0], key[1], owner, epoch),
        )

    def get_assignments(
        self, group_id: str, worker_id: str | None = None
    ) -> list[PartitionAssignment]:
        with self._connect() as connection:
            return self._get_assignments(connection, group_id, worker_id)

    @staticmethod
    def _get_assignments(
        connection: sqlite3.Connection, group_id: str, worker_id: str | None = None
    ) -> list[PartitionAssignment]:
        query = """
            SELECT a.group_id, a.topic, a.partition_id, a.worker_id, a.epoch, a.state,
                   p.broker_id, n.host, n.port
            FROM assignments AS a
            JOIN partitions AS p
              ON p.topic = a.topic AND p.partition_id = a.partition_id
            JOIN nodes AS n ON n.node_id = p.broker_id
            WHERE a.group_id = ?
        """
        params: tuple[object, ...] = (group_id,)
        if worker_id is not None:
            query += " AND a.worker_id = ?"
            params = (group_id, worker_id)
        query += " ORDER BY a.topic, a.partition_id"
        rows = connection.execute(query, params).fetchall()
        return [
            PartitionAssignment(
                group_id=row["group_id"],
                topic=row["topic"],
                partition_id=row["partition_id"],
                worker_id=row["worker_id"],
                broker_id=row["broker_id"],
                broker_host=row["host"],
                broker_port=row["port"],
                epoch=row["epoch"],
                state=row["state"],
            )
            for row in rows
        ]

    def _topic_partitions(
        self, connection: sqlite3.Connection, topic: str
    ) -> list[dict[str, object]]:
        rows = connection.execute(
            """
            SELECT p.topic, p.partition_id, p.broker_id, n.host AS broker_host,
                   n.port AS broker_port, t.partition_count
            FROM partitions AS p
            JOIN topics AS t ON t.name = p.topic
            LEFT JOIN nodes AS n ON n.node_id = p.broker_id
            WHERE p.topic = ?
            ORDER BY p.partition_id
            """,
            (topic,),
        ).fetchall()
        return [dict(row) for row in rows]

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    role TEXT NOT NULL CHECK(role IN ('broker', 'worker')),
                    host TEXT NOT NULL,
                    port INTEGER NOT NULL CHECK(port BETWEEN 0 AND 65535),
                    capacity REAL NOT NULL CHECK(capacity > 0),
                    status TEXT NOT NULL CHECK(status IN ('STARTING', 'HEALTHY', 'UNAVAILABLE')),
                    last_heartbeat_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS topics (
                    name TEXT PRIMARY KEY,
                    partition_count INTEGER NOT NULL CHECK(partition_count > 0),
                    created_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS partitions (
                    topic TEXT NOT NULL REFERENCES topics(name),
                    partition_id INTEGER NOT NULL CHECK(partition_id >= 0),
                    broker_id TEXT NOT NULL REFERENCES nodes(node_id),
                    PRIMARY KEY(topic, partition_id)
                );
                CREATE TABLE IF NOT EXISTS group_topics (
                    group_id TEXT NOT NULL,
                    topic TEXT NOT NULL REFERENCES topics(name),
                    PRIMARY KEY(group_id, topic)
                );
                CREATE TABLE IF NOT EXISTS group_members (
                    group_id TEXT NOT NULL,
                    worker_id TEXT NOT NULL REFERENCES nodes(node_id),
                    PRIMARY KEY(group_id, worker_id)
                );
                CREATE TABLE IF NOT EXISTS assignments (
                    group_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    worker_id TEXT NOT NULL REFERENCES nodes(node_id),
                    epoch INTEGER NOT NULL CHECK(epoch > 0),
                    state TEXT NOT NULL DEFAULT 'ACTIVE'
                        CHECK(state IN ('ACTIVE', 'REVOKING', 'UNASSIGNED')),
                    revoke_acked INTEGER NOT NULL DEFAULT 0 CHECK(revoke_acked IN (0, 1)),
                    PRIMARY KEY(group_id, topic, partition_id),
                    FOREIGN KEY(topic, partition_id)
                        REFERENCES partitions(topic, partition_id)
                );
                CREATE TABLE IF NOT EXISTS node_health (
                    node_id TEXT PRIMARY KEY REFERENCES nodes(node_id),
                    status TEXT NOT NULL CHECK(
                        status IN ('STARTING', 'HEALTHY', 'SUSPECTED', 'UNAVAILABLE')
                    )
                );
                CREATE TABLE IF NOT EXISTS node_metrics (
                    node_id TEXT PRIMARY KEY REFERENCES nodes(node_id),
                    instance_id TEXT NOT NULL,
                    timestamp_ms INTEGER NOT NULL,
                    sample_interval_ms INTEGER NOT NULL CHECK(sample_interval_ms >= 0),
                    cpu_percent REAL NOT NULL CHECK(cpu_percent BETWEEN 0 AND 100),
                    memory_percent REAL NOT NULL CHECK(memory_percent BETWEEN 0 AND 100),
                    queue_depth INTEGER NOT NULL CHECK(queue_depth >= 0),
                    queue_capacity INTEGER NOT NULL CHECK(queue_capacity > 0),
                    p95_latency_ms REAL NOT NULL CHECK(p95_latency_ms >= 0),
                    processing_rate REAL NOT NULL CHECK(processing_rate >= 0),
                    in_flight INTEGER NOT NULL CHECK(in_flight >= 0),
                    lag_records INTEGER NOT NULL CHECK(lag_records >= 0),
                    raw_pressure REAL NOT NULL CHECK(raw_pressure BETWEEN 0 AND 1),
                    ema_pressure REAL NOT NULL CHECK(ema_pressure BETWEEN 0 AND 1),
                    overloaded INTEGER NOT NULL CHECK(overloaded IN (0, 1)),
                    components_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS metric_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT NOT NULL REFERENCES nodes(node_id),
                    instance_id TEXT NOT NULL,
                    timestamp_ms INTEGER NOT NULL,
                    sample_interval_ms INTEGER NOT NULL CHECK(sample_interval_ms >= 0),
                    cpu_percent REAL NOT NULL,
                    memory_percent REAL NOT NULL,
                    queue_depth INTEGER NOT NULL,
                    queue_capacity INTEGER NOT NULL,
                    p95_latency_ms REAL NOT NULL,
                    processing_rate REAL NOT NULL,
                    in_flight INTEGER NOT NULL,
                    lag_records INTEGER NOT NULL,
                    raw_pressure REAL NOT NULL,
                    ema_pressure REAL NOT NULL,
                    overloaded INTEGER NOT NULL,
                    components_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS metric_history_node_time
                    ON metric_history(node_id, timestamp_ms);
                CREATE TABLE IF NOT EXISTS partition_metrics (
                    group_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    worker_id TEXT NOT NULL REFERENCES nodes(node_id),
                    lag_records INTEGER NOT NULL CHECK(lag_records >= 0),
                    timestamp_ms INTEGER NOT NULL,
                    PRIMARY KEY(group_id, topic, partition_id, worker_id)
                );
                CREATE TABLE IF NOT EXISTS optimizer_decisions (
                    decision_id TEXT PRIMARY KEY,
                    group_id TEXT NOT NULL,
                    decision_type TEXT NOT NULL
                        CHECK(decision_type IN ('MOVE_PARTITION', 'NO_ACTION')),
                    status TEXT NOT NULL
                        CHECK(status IN ('PENDING', 'MOVED', 'VERIFIED', 'NO_ACTION', 'FAILED')),
                    reason TEXT NOT NULL,
                    created_ms INTEGER NOT NULL,
                    source_worker TEXT,
                    destination_worker TEXT,
                    topic TEXT,
                    partition_id INTEGER,
                    predicted_benefit REAL,
                    source_pressure_before REAL,
                    destination_pressure_before REAL,
                    moved_ms INTEGER,
                    verified_ms INTEGER,
                    actual_benefit REAL,
                    prediction_error REAL,
                    details_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS optimizer_decisions_group_time
                    ON optimizer_decisions(group_id, created_ms);
                CREATE TABLE IF NOT EXISTS optimizer_moves (
                    decision_id TEXT PRIMARY KEY REFERENCES optimizer_decisions(decision_id),
                    group_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    partition_id INTEGER NOT NULL,
                    source_worker TEXT NOT NULL REFERENCES nodes(node_id),
                    destination_worker TEXT NOT NULL REFERENCES nodes(node_id),
                    state TEXT NOT NULL CHECK(state IN ('PENDING', 'MOVED', 'FAILED'))
                );
                """
            )
            assignment_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(assignments)")
            }
            if "state" not in assignment_columns:
                connection.execute(
                    """
                    ALTER TABLE assignments ADD COLUMN state TEXT NOT NULL DEFAULT 'ACTIVE'
                    CHECK(state IN ('ACTIVE', 'REVOKING', 'UNASSIGNED'))
                    """
                )
            if "revoke_acked" not in assignment_columns:
                connection.execute(
                    """
                    ALTER TABLE assignments ADD COLUMN revoke_acked INTEGER NOT NULL DEFAULT 0
                    CHECK(revoke_acked IN (0, 1))
                    """
                )
            connection.execute(
                """
                INSERT OR IGNORE INTO node_health(node_id, status)
                SELECT node_id,
                       CASE WHEN status = 'UNAVAILABLE' THEN 'UNAVAILABLE' ELSE 'HEALTHY' END
                FROM nodes
                """
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    @staticmethod
    def _node_from_row(row: sqlite3.Row) -> NodeRecord:
        return NodeRecord(
            node_id=row["node_id"],
            role=row["role"],
            host=row["host"],
            port=row["port"],
            capacity=row["capacity"],
            status=row["health_status"],
            last_heartbeat_ms=row["last_heartbeat_ms"],
        )

    @staticmethod
    def _validate_node(node_id: str, role: str, host: str, port: int, capacity: float) -> None:
        if not node_id or len(node_id) > 128:
            raise ValueError("node_id must contain 1 to 128 characters")
        if role not in {"broker", "worker"}:
            raise ValueError("role must be 'broker' or 'worker'")
        if not host or len(host) > 255:
            raise ValueError("host must contain 1 to 255 characters")
        if isinstance(port, bool) or not isinstance(port, int) or not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        if (
            isinstance(capacity, bool)
            or not isinstance(capacity, (int, float))
            or not isfinite(capacity)
            or capacity <= 0
        ):
            raise ValueError("capacity must be positive")
