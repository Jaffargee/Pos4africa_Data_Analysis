"""PosWriteSaga: the one place that knows how to change POS safely.

Every feature (customers, sales, ...) plugs in a ``WriteHandler`` and gets the same
guarantees:

  * write-ahead intent  - the request is in the log before POS is touched
  * idempotency         - same key replays the result; same key + different body is refused
  * single sender       - a compare-and-set claim, so concurrent duplicates send once
  * honest failure      - "POS said no" / "never sent" / "might have been applied" are
                          different states with different recovery paths

The server never writes business data to Supabase: once POS has the write, the ETL
carries it into Supabase like every other POS change.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol
from uuid import uuid4

from server.domain.errors import (
    ConflictError,
    PosOutcomeUnknownError,
    PosRejectedError,
    PosUnavailableError,
)
from server.domain.ports import WriteLogRepository
from server.domain.write_log import WriteKind, WriteLogEntry, WriteStatus

log = logging.getLogger(__name__)

_CLAIMABLE = (WriteStatus.PENDING, WriteStatus.FAILED)


class WriteHandler(Protocol):
    kind: WriteKind
    idempotent: bool  # True: re-sending after an unknown outcome is harmless (e.g. updates)

    async def send(self, entry: WriteLogEntry, *, commit: bool) -> int | None:
        """Change POS.  Returns the POS id, or None when ``commit`` is False."""

    async def resolve(self, entry: WriteLogEntry) -> int | None:
        """After an unknown outcome: the POS id if the write landed, None if provably not."""


@dataclass
class WriteResult:
    ref: str
    kind: WriteKind
    status: WriteStatus | None  # None for a dry run (nothing was logged)
    pos_id: int | None = None
    entry: WriteLogEntry | None = None
    replayed: bool = False
    dry_run: bool = False
    error: str | None = None


def _result(entry: WriteLogEntry, *, replayed: bool = False) -> WriteResult:
    return WriteResult(
        ref=entry.ref, kind=entry.kind, status=entry.status, pos_id=entry.pos_id,
        entry=entry, replayed=replayed, error=entry.error,
    )


class PosWriteSaga:
    def __init__(self, write_log: WriteLogRepository, handlers: dict[WriteKind, WriteHandler], mode: str):
        self._log = write_log
        self._handlers = handlers
        self._mode = mode  # "dry_run" | "commit"

    @property
    def mode(self) -> str:
        return self._mode

    # -- idempotent replay ---------------------------------------------------------
    async def replay_if_done(self, ref: str, request_hash: str) -> WriteResult | None:
        """Cheap pre-check so a finished request replays without re-validating."""
        entry = await self._log.get_by_ref(ref)
        if entry is None:
            return None
        self._check_same_request(entry, request_hash)
        return _result(entry, replayed=True) if entry.status is WriteStatus.POSTED else None

    @staticmethod
    def _check_same_request(entry: WriteLogEntry, request_hash: str) -> None:
        if entry.request_hash != request_hash:
            raise ConflictError("This Idempotency-Key was already used with a different request body")

    # -- execute ---------------------------------------------------------------------
    async def execute(self, *, kind: WriteKind, ref: str, request_hash: str, payload: dict) -> WriteResult:
        handler = self._handlers[kind]

        if self._mode != "commit":
            now = datetime.now(timezone.utc)
            draft = WriteLogEntry(
                id=uuid4(), kind=kind, ref=ref, request_hash=request_hash, status=WriteStatus.PENDING,
                payload=payload, created_at=now, updated_at=now,
            )
            await handler.send(draft, commit=False)  # rehearse; handler must not change POS
            return WriteResult(ref=ref, kind=kind, status=None, dry_run=True)

        entry, created = await self._log.begin(kind=kind, ref=ref, request_hash=request_hash, payload=payload)
        if not created:
            self._check_same_request(entry, request_hash)
            status = entry.status
            if status is WriteStatus.POSTED:
                return _result(entry, replayed=True)
            if status in (WriteStatus.SENDING, WriteStatus.UNCERTAIN):
                raise ConflictError(
                    f"Request {ref} is {status.value}: it may already have reached POS. "
                    "It will be resolved by reconciliation; do not resend with a new key."
                )
            if status is WriteStatus.FAILED and not entry.retryable:
                raise PosRejectedError(entry.error or "POS refused this request earlier")
            # PENDING (crashed before sending) or FAILED+retryable: fall through and send

        entry = await self._log.transition(entry.id, _CLAIMABLE, WriteStatus.SENDING)  # CAS claim

        try:
            pos_id = await handler.send(entry, commit=True)
            if pos_id is None:
                raise PosOutcomeUnknownError("POS write returned no id")
        except PosRejectedError as exc:
            await self._settle(entry, WriteStatus.FAILED, error=str(exc), retryable=False)
            raise
        except PosUnavailableError as exc:
            await self._settle(entry, WriteStatus.FAILED, error=str(exc), retryable=True)
            raise
        except Exception as exc:  # unknown outcome, including bugs: assume it may have landed
            if handler.idempotent:
                await self._settle(entry, WriteStatus.FAILED, error=str(exc), retryable=True)
            else:
                await self._settle(entry, WriteStatus.UNCERTAIN, error=str(exc))
            if isinstance(exc, PosOutcomeUnknownError):
                raise
            raise PosOutcomeUnknownError(str(exc)) from exc

        entry = await self._log.transition(entry.id, (WriteStatus.SENDING,), WriteStatus.POSTED, pos_id=pos_id)
        return _result(entry)

    async def _settle(self, entry: WriteLogEntry, status: WriteStatus, *, error: str, retryable: bool | None = None) -> None:
        try:
            await self._log.transition(entry.id, (WriteStatus.SENDING,), status, error=error, retryable=retryable)
        except Exception:  # the log write itself failed: entry stays SENDING, reconcile picks it up
            log.exception("could not record %s for %s", status, entry.ref)

    # -- reconcile ---------------------------------------------------------------------
    async def reconcile(self, entry: WriteLogEntry) -> WriteLogEntry:
        """Bring one stuck entry to a final state without ever sending a second write."""
        if entry.status not in (WriteStatus.SENDING, WriteStatus.UNCERTAIN):
            return entry
        expected = (WriteStatus.SENDING, WriteStatus.UNCERTAIN)
        pos_id = await self._handlers[entry.kind].resolve(entry)
        if pos_id is None:
            return await self._log.transition(
                entry.id, expected, WriteStatus.FAILED,
                error="not found in POS during reconciliation", retryable=True,
            )
        return await self._log.transition(entry.id, expected, WriteStatus.POSTED, pos_id=pos_id, error="")

    async def reconcile_open(self, grace: timedelta) -> list[WriteLogEntry]:
        cutoff = datetime.now(timezone.utc) - grace
        out: list[WriteLogEntry] = []
        for entry in await self._log.list_open(cutoff):
            try:
                out.append(await self.reconcile(entry))
            except Exception:
                log.exception("reconcile failed for %s; will retry next pass", entry.ref)
        return out
