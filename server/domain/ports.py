"""Ports shared by every feature.  Implemented in ``infrastructure/``."""

from __future__ import annotations

from collections.abc import Collection
from datetime import datetime
from typing import Protocol
from uuid import UUID

from server.domain.write_log import WriteKind, WriteLogEntry, WriteStatus


class WriteLogRepository(Protocol):
    async def begin(
        self, *, kind: WriteKind, ref: str, request_hash: str, payload: dict
    ) -> tuple[WriteLogEntry, bool]:
        """Insert a PENDING row, or return the existing row for ``ref``.

        Returns ``(entry, created)``.  Must be race-safe: two concurrent calls with
        the same ref produce exactly one ``created=True``.
        """

    async def transition(
        self,
        entry_id: UUID,
        expected: Collection[WriteStatus],
        new: WriteStatus,
        *,
        pos_id: int | None = None,
        error: str | None = None,
        retryable: bool | None = None,
    ) -> WriteLogEntry:
        """Compare-and-set the status.  Raises ConflictError if the row is not in
        one of the ``expected`` states (someone else moved it first)."""

    async def get_by_ref(self, ref: str) -> WriteLogEntry | None: ...

    async def list(
        self, *, status: WriteStatus | None = None, kind: WriteKind | None = None, limit: int = 50
    ) -> list[WriteLogEntry]: ...

    async def list_open(self, older_than: datetime) -> list[WriteLogEntry]:
        """Entries in SENDING / UNCERTAIN / POSTED not touched since ``older_than``."""
