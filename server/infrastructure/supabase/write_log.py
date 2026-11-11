from __future__ import annotations

import asyncio
from collections.abc import Collection
from datetime import datetime
from uuid import UUID

from postgrest.exceptions import APIError

from server.domain.errors import ConflictError
from server.domain.write_log import WriteKind, WriteLogEntry, WriteStatus, assert_transition, OPEN_STATUSES
from server.infrastructure.supabase._client import get_client

TABLE = "pos_write_log"


def _entry(row: dict) -> WriteLogEntry:
    return WriteLogEntry.model_validate(row)


class SupabaseWriteLog:
    def __init__(self, client_factory=get_client):
        self._client = client_factory

    async def begin(self, *, kind: WriteKind, ref: str, request_hash: str, payload: dict):
        row = {"kind": kind.value, "ref": ref, "request_hash": request_hash, "payload": payload}

        def _do():
            c = self._client()
            try:
                return _entry(c.table(TABLE).insert(row).execute().data[0]), True
            except APIError as exc:
                if str(getattr(exc, "code", "")) != "23505":  # unique_violation: ref already logged
                    raise
                existing = c.table(TABLE).select("*").eq("ref", ref).limit(1).execute().data[0]
                return _entry(existing), False

        return await asyncio.to_thread(_do)

    async def transition(
        self, entry_id: UUID, expected: Collection[WriteStatus], new: WriteStatus, *,
        pos_id: int | None = None, error: str | None = None, retryable: bool | None = None,
    ) -> WriteLogEntry:
        for old in expected:
            assert_transition(old, new)
        update: dict = {"status": new.value}
        if pos_id is not None:
            update["pos_id"] = pos_id
        if error is not None:
            update["error"] = error or None  # "" clears it
        if retryable is not None:
            update["retryable"] = retryable

        def _do():
            c = self._client()
            if new is WriteStatus.SENDING:
                cur = c.table(TABLE).select("attempts").eq("id", str(entry_id)).limit(1).execute().data
                update["attempts"] = (cur[0]["attempts"] if cur else 0) + 1
            res = (
                c.table(TABLE).update(update).eq("id", str(entry_id))
                .in_("status", [s.value for s in expected]).execute()
            )
            if not res.data:
                raise ConflictError(f"write-log entry {entry_id} is no longer in {sorted(s.value for s in expected)}")
            return _entry(res.data[0])

        return await asyncio.to_thread(_do)

    async def get_by_ref(self, ref: str) -> WriteLogEntry | None:
        def _do():
            rows = self._client().table(TABLE).select("*").eq("ref", ref).limit(1).execute().data
            return _entry(rows[0]) if rows else None

        return await asyncio.to_thread(_do)

    async def list(self, *, status: WriteStatus | None = None, kind: WriteKind | None = None, limit: int = 50):
        def _do():
            q = self._client().table(TABLE).select("*").order("created_at", desc=True).limit(limit)
            if status:
                q = q.eq("status", status.value)
            if kind:
                q = q.eq("kind", kind.value)
            return [_entry(r) for r in q.execute().data]

        return await asyncio.to_thread(_do)

    async def list_open(self, older_than: datetime) -> list[WriteLogEntry]:
        def _do():
            q = (
                self._client().table(TABLE).select("*")
                .in_("status", [s.value for s in OPEN_STATUSES])
                .lt("updated_at", older_than.isoformat()).order("created_at").limit(200)
            )
            return [_entry(r) for r in q.execute().data]

        return await asyncio.to_thread(_do)
