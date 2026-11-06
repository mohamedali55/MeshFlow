"""MeshFlow coordinator and cluster metadata."""

from typing import TYPE_CHECKING

from meshflow.coordinator.client import CoordinatorClient
from meshflow.coordinator.metadata import MetadataStore, NodeRecord, PartitionAssignment

if TYPE_CHECKING:
    from meshflow.coordinator.server import CoordinatorServer

__all__ = [
    "CoordinatorClient",
    "CoordinatorServer",
    "MetadataStore",
    "NodeRecord",
    "PartitionAssignment",
]


def __getattr__(name: str):
    if name == "CoordinatorServer":
        from meshflow.coordinator.server import CoordinatorServer

        return CoordinatorServer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
