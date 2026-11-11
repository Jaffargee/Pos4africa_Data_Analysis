from __future__ import annotations

from datetime import timedelta

from server.application.pos_write import PosWriteSaga
from server.domain.errors import NotFoundError
from server.domain.ports import WriteLogRepository
from server.domain.write_log import WriteLogEntry


class Reconciler:
    """Operator-facing wrapper around the saga's reconcile logic."""

    def __init__(self, saga: PosWriteSaga, write_log: WriteLogRepository, grace_seconds: int):
        self._saga = saga
        self._log = write_log
        self._grace = timedelta(seconds=grace_seconds)

    async def one(self, ref: str) -> WriteLogEntry:
        entry = await self._log.get_by_ref(ref)
        if entry is None:
            raise NotFoundError(f"No write-log entry with ref {ref}")
        return await self._saga.reconcile(entry)

    async def all_open(self) -> list[WriteLogEntry]:
        return await self._saga.reconcile_open(self._grace)
