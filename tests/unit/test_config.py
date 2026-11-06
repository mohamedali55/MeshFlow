"""Unit tests for meshflow.common.config."""

from __future__ import annotations

from pathlib import Path

import pytest

from meshflow.common.config import BrokerConfig

# ── BrokerConfig defaults ────────────────────────────────────────────


class TestBrokerConfigDefaults:
    """Tests for BrokerConfig default values."""

    def test_default_host(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/tmp/data"))
        assert cfg.host == "127.0.0.1"

    def test_default_port(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/tmp/data"))
        assert cfg.port == 9500

    def test_default_segment_max_bytes(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/tmp/data"))
        assert cfg.segment_max_bytes == 64 * 1024 * 1024

    def test_default_segment_max_age_ms(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/tmp/data"))
        assert cfg.segment_max_age_ms == 3600 * 1000

    def test_default_index_interval_bytes(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/tmp/data"))
        assert cfg.index_interval_bytes == 4096

    def test_default_fsync_policy(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/tmp/data"))
        assert cfg.fsync_policy == "every_write"

    def test_default_fsync_interval_ms(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/tmp/data"))
        assert cfg.fsync_interval_ms == 1000

    def test_default_max_record_bytes(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/tmp/data"))
        assert cfg.max_record_bytes == 1 * 1024 * 1024

    def test_default_max_frame_bytes(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/tmp/data"))
        assert cfg.max_frame_bytes == 4 * 1024 * 1024


# ── BrokerConfig validation ─────────────────────────────────────────


class TestBrokerConfigValidation:
    """Tests for BrokerConfig __post_init__ validation."""

    def test_invalid_fsync_policy_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid fsync_policy"):
            BrokerConfig(
                broker_id="b0",
                data_dir=Path("/tmp/data"),
                fsync_policy="never",
            )

    def test_valid_fsync_policies_accepted(self) -> None:
        for policy in ("every_write", "interval", "os"):
            cfg = BrokerConfig(
                broker_id="b0",
                data_dir=Path("/tmp/data"),
                fsync_policy=policy,
            )
            assert cfg.fsync_policy == policy

    def test_non_positive_segment_max_bytes_raises(self) -> None:
        with pytest.raises(ValueError, match="segment_max_bytes must be positive"):
            BrokerConfig(
                broker_id="b0",
                data_dir=Path("/tmp/data"),
                segment_max_bytes=0,
            )

    def test_negative_segment_max_bytes_raises(self) -> None:
        with pytest.raises(ValueError, match="segment_max_bytes must be positive"):
            BrokerConfig(
                broker_id="b0",
                data_dir=Path("/tmp/data"),
                segment_max_bytes=-1,
            )

    def test_non_positive_max_record_bytes_raises(self) -> None:
        with pytest.raises(ValueError, match="max_record_bytes must be positive"):
            BrokerConfig(
                broker_id="b0",
                data_dir=Path("/tmp/data"),
                max_record_bytes=0,
            )

    def test_negative_max_record_bytes_raises(self) -> None:
        with pytest.raises(ValueError, match="max_record_bytes must be positive"):
            BrokerConfig(
                broker_id="b0",
                data_dir=Path("/tmp/data"),
                max_record_bytes=-10,
            )

    def test_non_positive_index_interval_bytes_raises(self) -> None:
        with pytest.raises(ValueError, match="index_interval_bytes must be positive"):
            BrokerConfig(
                broker_id="b0",
                data_dir=Path("/tmp/data"),
                index_interval_bytes=0,
            )

    def test_negative_index_interval_bytes_raises(self) -> None:
        with pytest.raises(ValueError, match="index_interval_bytes must be positive"):
            BrokerConfig(
                broker_id="b0",
                data_dir=Path("/tmp/data"),
                index_interval_bytes=-1,
            )

    @pytest.mark.parametrize(
        ("setting", "value", "message"),
        [
            ("segment_max_age_ms", 0, "segment_max_age_ms must be positive"),
            ("fsync_interval_ms", 0, "fsync_interval_ms must be positive"),
            ("max_frame_bytes", 0, "max_frame_bytes must be positive"),
            ("port", -1, "port must be between 0 and 65535"),
            ("port", 65536, "port must be between 0 and 65535"),
        ],
    )
    def test_invalid_numeric_settings_raise(self, setting: str, value: int, message: str) -> None:
        with pytest.raises(ValueError, match=message):
            BrokerConfig(
                broker_id="b0",
                data_dir=Path("/tmp/data"),
                **{setting: value},
            )


# ── BrokerConfig directory properties ───────────────────────────────


class TestBrokerConfigDirectories:
    """Tests for directory path properties."""

    def test_topics_dir(self) -> None:
        cfg = BrokerConfig(broker_id="broker-1", data_dir=Path("/data"))
        assert cfg.topics_dir == Path("/data/broker-1/topics")

    def test_metadata_dir(self) -> None:
        cfg = BrokerConfig(broker_id="broker-1", data_dir=Path("/data"))
        assert cfg.metadata_dir == Path("/data/broker-1/metadata")

    def test_partition_dir(self) -> None:
        cfg = BrokerConfig(broker_id="broker-1", data_dir=Path("/data"))
        expected = Path("/data/broker-1/topics/orders/partition-003")
        assert cfg.partition_dir("orders", 3) == expected

    def test_partition_dir_zero_padded(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/data"))
        result = cfg.partition_dir("t", 0)
        assert result.name == "partition-000"

    def test_partition_dir_large_id(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/data"))
        result = cfg.partition_dir("t", 100)
        assert result.name == "partition-100"


# ── String data_dir auto-conversion ─────────────────────────────────


class TestBrokerConfigStringConversion:
    """Tests that string data_dir is converted to Path."""

    def test_string_data_dir_converted_to_path(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir="/tmp/meshflow")  # type: ignore[arg-type]
        assert isinstance(cfg.data_dir, Path)
        assert cfg.data_dir == Path("/tmp/meshflow")

    def test_path_data_dir_remains_path(self) -> None:
        cfg = BrokerConfig(broker_id="b0", data_dir=Path("/tmp/meshflow"))
        assert isinstance(cfg.data_dir, Path)
