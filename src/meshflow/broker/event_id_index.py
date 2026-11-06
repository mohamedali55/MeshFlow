"""Rebuildable disk-backed event-ID lookup for publish idempotency."""

from __future__ import annotations

import logging
import sqlite3
import threading
from collections.abc import Iterable
from pathlib import Path

from meshflow.broker.serialization import Record

logger = logging.getLogger(__name__)
_BATCH_SIZE = 512


class EventIdIndex:
    """Maps retained event IDs to offsets without retaining the full map in memory."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.RLock()
        self._connection: sqlite3.Connection | None = None

    def lookup(self, event_id: str) -> int | None:
        with self._lock:
            connection = self._connect()
            row = connection.execute(
                "SELECT offset FROM event_ids WHERE event_id = ?", (event_id,)
            ).fetchone()
            return int(row[0]) if row is not None else None

    def add(self, event_id: str, offset: int) -> None:
        with self._lock:
            connection = self._connect()
            with connection:
                connection.execute(
                    "INSERT OR IGNORE INTO event_ids(event_id, offset) VALUES (?, ?)",
                    (event_id, offset),
                )

    def rebuild(self, records: Iterable[Record]) -> None:
        with self._lock:
            connection = self._connect()
            with connection:
                connection.execute("DROP TABLE IF EXISTS event_ids")
                connection.execute(
                    """
                    CREATE TABLE event_ids (
                        event_id TEXT PRIMARY KEY,
                        offset INTEGER NOT NULL
                    )
                    """
                )
                batch: list[tuple[str, int]] = []
                for record in records:
                    batch.append((record.event_id, record.offset))
                    if len(batch) == _BATCH_SIZE:
                        connection.executemany(
                            "INSERT OR IGNORE INTO event_ids(event_id, offset) VALUES (?, ?)",
                            batch,
                        )
                        batch.clear()
                if batch:
                    connection.executemany(
                        "INSERT OR IGNORE INTO event_ids(event_id, offset) VALUES (?, ?)",
                        batch,
                    )

    def close(self) -> None:
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    def _connect(self) -> sqlite3.Connection:
        if self._connection is not None:
            return self._connection

        self._path.parent.mkdir(parents=True, exist_ok=True)
        try:
            connection = sqlite3.connect(self._path, check_same_thread=False)
            connection.execute("PRAGMA journal_mode = DELETE")
            connection.execute("PRAGMA synchronous = NORMAL")
            check = connection.execute("PRAGMA quick_check").fetchone()
            if check is None or check[0] != "ok":
                raise sqlite3.DatabaseError("event ID index integrity check failed")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS event_ids (
                    event_id TEXT PRIMARY KEY,
                    offset INTEGER NOT NULL
                )
                """
            )
        except sqlite3.DatabaseError:
            if "connection" in locals():
                connection.close()
            self._remove_corrupt_index()
            logger.warning("Rebuilding damaged event ID index at %s", self._path)
            connection = sqlite3.connect(self._path, check_same_thread=False)
            connection.execute("PRAGMA journal_mode = DELETE")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.execute(
                """
                CREATE TABLE event_ids (
                    event_id TEXT PRIMARY KEY,
                    offset INTEGER NOT NULL
                )
                """
            )
        self._connection = connection
        return connection

    def _remove_corrupt_index(self) -> None:
        for path in (
            self._path,
            self._path.with_name(f"{self._path.name}-journal"),
            self._path.with_name(f"{self._path.name}-wal"),
            self._path.with_name(f"{self._path.name}-shm"),
        ):
            try:
                path.unlink(missing_ok=True)
            except OSError:
                logger.exception("Could not remove damaged event ID index file %s", path)
                raise
