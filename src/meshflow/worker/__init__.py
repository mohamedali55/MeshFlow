"""MeshFlow worker process and assignment runner."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from meshflow.worker.process import PermanentHandlerError, Worker

__all__ = ["PermanentHandlerError", "Worker"]


def __getattr__(name: str):
    if name == "Worker":
        from meshflow.worker.process import Worker

        return Worker
    if name == "PermanentHandlerError":
        from meshflow.worker.process import PermanentHandlerError

        return PermanentHandlerError
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
