from __future__ import annotations

from fastapi import Response
from pydantic import BaseModel

from server.application.pos_write import WriteResult
from server.domain.write_log import WriteKind, WriteLogEntry, WriteStatus


class WriteResponse(BaseModel):
    ref: str
    kind: WriteKind
    status: WriteStatus | None  # null on a dry run
    pos_id: int | None = None
    replayed: bool = False
    dry_run: bool = False
    error: str | None = None


def write_response(result: WriteResult, response: Response) -> WriteResponse:
    """201 created | 200 replay or dry run | 202 POS has it, local mirror still pending."""
    if result.replayed or result.dry_run:
        response.status_code = 200
    elif result.status is WriteStatus.POSTED:
        response.status_code = 202
    else:
        response.status_code = 201
    return WriteResponse(
        ref=result.ref, kind=result.kind, status=result.status, pos_id=result.pos_id,
        replayed=result.replayed, dry_run=result.dry_run, error=result.error,
    )


class ReconEntry(BaseModel):
    ref: str
    kind: WriteKind
    status: WriteStatus
    pos_id: int | None
    error: str | None
    retryable: bool
    attempts: int

    @classmethod
    def of(cls, e: WriteLogEntry) -> "ReconEntry":
        return cls(ref=e.ref, kind=e.kind, status=e.status, pos_id=e.pos_id, error=e.error,
                   retryable=e.retryable, attempts=e.attempts)
