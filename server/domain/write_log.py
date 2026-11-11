"""The write log: one row per attempt to change POS4Africa.

Every server write that touches POS goes through this log *first* (write-ahead),
so after any crash we know what we intended, how far we got, and what is safe
to do next.

    PENDING --> SENDING --> POSTED --> SYNCED          happy path
                   |           (POS has it, local commit pending)
                   +--> FAILED     POS refused, or never received it
                   +--> UNCERTAIN  sent, outcome unknown -> reconcile, never blind retry

    UNCERTAIN --> POSTED (found in POS) | FAILED (provably absent)
    FAILED    --> SENDING only when retryable
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class WriteKind(StrEnum):
    CUSTOMER_CREATE = "CUSTOMER_CREATE"
    CUSTOMER_UPDATE = "CUSTOMER_UPDATE"
    SALE_CREATE = "SALE_CREATE"


class WriteStatus(StrEnum):
    PENDING = "PENDING"
    SENDING = "SENDING"
    POSTED = "POSTED"
    SYNCED = "SYNCED"
    FAILED = "FAILED"
    UNCERTAIN = "UNCERTAIN"


_ALLOWED: dict[WriteStatus, set[WriteStatus]] = {
    WriteStatus.PENDING: {WriteStatus.SENDING, WriteStatus.FAILED},
    WriteStatus.SENDING: {WriteStatus.POSTED, WriteStatus.FAILED, WriteStatus.UNCERTAIN},
    WriteStatus.POSTED: {WriteStatus.POSTED, WriteStatus.SYNCED},  # POSTED->POSTED = local commit retried
    WriteStatus.UNCERTAIN: {WriteStatus.POSTED, WriteStatus.FAILED},
    WriteStatus.FAILED: {WriteStatus.SENDING},
    WriteStatus.SYNCED: set(),
}

# States that mean "something may be half-done and needs attention".
OPEN_STATUSES = (WriteStatus.SENDING, WriteStatus.UNCERTAIN, WriteStatus.POSTED)


def assert_transition(old: WriteStatus, new: WriteStatus) -> None:
    if new not in _ALLOWED[old]:
        raise ValueError(f"illegal write-log transition {old} -> {new}")


class WriteLogEntry(BaseModel):
    id: UUID
    kind: WriteKind
    ref: str                      # unique; derived from the client's Idempotency-Key
    request_hash: str             # fingerprint of the request body (detects key reuse)
    status: WriteStatus
    payload: dict[str, Any]       # everything needed to (re)send or (re)commit
    pos_id: int | None = None     # POS id once known (person_id / sale code)
    error: str | None = None
    retryable: bool = False
    attempts: int = 0
    created_at: datetime
    updated_at: datetime
