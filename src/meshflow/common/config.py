"""Configuration for MeshFlow components.

All configuration values have documented defaults. These are development
starting points, not universal production values.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class BrokerConfig:
    """Configuration for a single broker process.

    Attributes:
        broker_id: Unique identifier for this broker.
        data_dir: Root directory for broker data storage.
        host: Bind address for the broker server.
        port: Bind port for the broker server.
        segment_max_bytes: Maximum segment size before rotation (default: 64 MB).
        segment_max_age_ms: Maximum segment age before rotation (default: 1 hour).
        index_interval_bytes: Bytes between sparse index entries (default: 4 KB).
        fsync_policy: When to fsync — "every_write", "interval", or "os".
        fsync_interval_ms: Interval between fsyncs when policy is "interval".
        max_record_bytes: Maximum allowed record size (default: 1 MB).
        max_frame_bytes: Maximum allowed network frame size (default: 4 MB).
    """

    broker_id: str
    data_dir: Path
    host: str = "127.0.0.1"
    port: int = 9500
    segment_max_bytes: int = 64 * 1024 * 1024  # 64 MB
    segment_max_age_ms: int = 3600 * 1000  # 1 hour
    index_interval_bytes: int = 4096  # index entry every 4 KB
    fsync_policy: str = "every_write"  # "every_write" | "interval" | "os"
    fsync_interval_ms: int = 1000
    max_record_bytes: int = 1 * 1024 * 1024  # 1 MB max record
    max_frame_bytes: int = 4 * 1024 * 1024  # 4 MB max frame

    def __post_init__(self) -> None:
        if isinstance(self.data_dir, str):
            self.data_dir = Path(self.data_dir)
        if self.fsync_policy not in ("every_write", "interval", "os"):
            raise ValueError(
                f"Invalid fsync_policy: {self.fsync_policy!r}. "
                "Must be 'every_write', 'interval', or 'os'."
            )
        if self.segment_max_bytes <= 0:
            raise ValueError("segment_max_bytes must be positive")
        if self.segment_max_age_ms <= 0:
            raise ValueError("segment_max_age_ms must be positive")
        if self.max_record_bytes <= 0:
            raise ValueError("max_record_bytes must be positive")
        if self.index_interval_bytes <= 0:
            raise ValueError("index_interval_bytes must be positive")
        if self.fsync_interval_ms <= 0:
            raise ValueError("fsync_interval_ms must be positive")
        if self.max_frame_bytes <= 0:
            raise ValueError("max_frame_bytes must be positive")
        if not 0 <= self.port <= 65535:
            raise ValueError("port must be between 0 and 65535")

    @property
    def topics_dir(self) -> Path:
        """Path to the topics data directory for this broker."""
        return self.data_dir / self.broker_id / "topics"

    @property
    def metadata_dir(self) -> Path:
        """Path to the metadata directory for this broker."""
        return self.data_dir / self.broker_id / "metadata"

    def partition_dir(self, topic: str, partition_id: int) -> Path:
        """Path to a specific partition's data directory."""
        return self.topics_dir / topic / f"partition-{partition_id:03d}"
