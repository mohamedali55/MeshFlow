"""Broker recovery: discovers partitions and recovers their logs on startup.

On broker startup:
1. Discover partition directories.
2. For each partition: validate segment headers, record framing, and checksums.
3. Rebuild sparse indexes where needed.
4. Recover the next valid offset per partition.
5. Handle incomplete trailing records by truncating only the incomplete tail.
6. If corruption is found in the middle of a segment, mark the partition damaged.

This module orchestrates recovery across all partitions for a broker.
Individual segment recovery is handled by LogSegment.recover().
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from meshflow.broker.segment import SegmentRecoveryResult
from meshflow.broker.storage import PartitionLog
from meshflow.common.config import BrokerConfig
from meshflow.common.types import PartitionStatus

logger = logging.getLogger(__name__)


@dataclass
class PartitionRecoveryResult:
    """Recovery result for a single partition."""

    topic: str
    partition_id: int
    status: PartitionStatus
    segment_results: list[SegmentRecoveryResult] = field(default_factory=list)
    total_records: int = 0
    next_offset: int = 0

    @property
    def is_healthy(self) -> bool:
        return self.status in (PartitionStatus.ACTIVE, PartitionStatus.RECOVERING)


@dataclass
class BrokerRecoveryResult:
    """Recovery result for the entire broker."""

    broker_id: str
    partitions: list[PartitionRecoveryResult] = field(default_factory=list)
    total_partitions: int = 0
    healthy_partitions: int = 0
    damaged_partitions: int = 0

    @property
    def is_healthy(self) -> bool:
        return self.damaged_partitions == 0


class BrokerRecovery:
    """Recovers all partition logs for a broker on startup."""

    def __init__(self, config: BrokerConfig) -> None:
        self._config = config
        self._partition_logs: dict[tuple[str, int], PartitionLog] = {}

    @property
    def partition_logs(self) -> dict[tuple[str, int], PartitionLog]:
        """Recovered partition logs keyed by (topic, partition_id)."""
        return dict(self._partition_logs)

    def recover(self) -> BrokerRecoveryResult:
        """Recover all partitions from disk.

        Scans the broker's topics directory, discovers all partition
        directories, and recovers each one.

        Returns:
            A BrokerRecoveryResult summarizing the recovery.
        """
        self._partition_logs.clear()
        result = BrokerRecoveryResult(broker_id=self._config.broker_id)
        topics_dir = self._config.topics_dir

        if not topics_dir.exists():
            logger.info(
                "No topics directory for broker %s — clean start",
                self._config.broker_id,
            )
            return result

        # Discover topics
        for topic_dir in sorted(topics_dir.iterdir()):
            if not topic_dir.is_dir():
                continue
            topic = topic_dir.name

            # Discover partitions within topic
            for partition_dir in sorted(topic_dir.iterdir()):
                if not partition_dir.is_dir():
                    continue
                if not partition_dir.name.startswith("partition-"):
                    continue

                try:
                    partition_id = int(partition_dir.name.split("-", 1)[1])
                except (ValueError, IndexError):
                    logger.warning("Skipping invalid partition directory: %s", partition_dir)
                    continue

                partition_result = self._recover_partition(topic, partition_id)
                result.partitions.append(partition_result)
                result.total_partitions += 1

                if partition_result.is_healthy:
                    result.healthy_partitions += 1
                else:
                    result.damaged_partitions += 1

        logger.info(
            "Broker %s recovery complete: %d partitions (%d healthy, %d damaged)",
            self._config.broker_id,
            result.total_partitions,
            result.healthy_partitions,
            result.damaged_partitions,
        )

        return result

    def _recover_partition(self, topic: str, partition_id: int) -> PartitionRecoveryResult:
        """Recover a single partition's log."""
        logger.info("Recovering partition %s/%d", topic, partition_id)

        partition_log = PartitionLog(
            topic=topic,
            partition_id=partition_id,
            config=self._config,
        )

        segment_results = partition_log.recover()

        partition_result = PartitionRecoveryResult(
            topic=topic,
            partition_id=partition_id,
            status=partition_log.status,
            segment_results=segment_results,
            total_records=partition_log.total_records,
            next_offset=partition_log.next_offset,
        )

        if partition_log.status != PartitionStatus.DAMAGED:
            self._partition_logs[(topic, partition_id)] = partition_log

        return partition_result

    def get_partition_log(self, topic: str, partition_id: int) -> PartitionLog | None:
        """Get a recovered partition log by topic and partition ID."""
        return self._partition_logs.get((topic, partition_id))
