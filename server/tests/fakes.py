"""In-memory implementations of every port, with knobs to inject failures."""

from __future__ import annotations

from collections.abc import Collection
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

from server.domain.customers.models import CustomerFields
from server.domain.errors import ConflictError
from server.domain.sales.models import FoundSale, ResolvedSale
from server.domain.write_log import (
    OPEN_STATUSES,
    WriteKind,
    WriteLogEntry,
    WriteStatus,
    assert_transition,
)


class InMemoryWriteLog:
    def __init__(self):
        self.rows: dict[UUID, WriteLogEntry] = {}

    async def begin(self, *, kind, ref, request_hash, payload):
        for row in self.rows.values():
            if row.ref == ref:
                return row.model_copy(), False
        now = datetime.now(timezone.utc)
        row = WriteLogEntry(
            id=uuid4(), kind=kind, ref=ref, request_hash=request_hash, status=WriteStatus.PENDING,
            payload=payload, created_at=now, updated_at=now,
        )
        self.rows[row.id] = row
        return row.model_copy(), True

    async def transition(self, entry_id, expected: Collection[WriteStatus], new, *, pos_id=None, error=None, retryable=None):
        row = self.rows[entry_id]
        if row.status not in expected:
            raise ConflictError(f"{entry_id} is {row.status}, expected one of {sorted(expected)}")
        assert_transition(row.status, new)
        if new is WriteStatus.SENDING:
            row.attempts += 1
        row.status = new
        if pos_id is not None:
            row.pos_id = pos_id
        if error is not None:
            row.error = error or None
        if retryable is not None:
            row.retryable = retryable
        row.updated_at = datetime.now(timezone.utc)
        return row.model_copy()

    async def get_by_ref(self, ref):
        return next((r.model_copy() for r in self.rows.values() if r.ref == ref), None)

    async def list(self, *, status=None, kind=None, limit=50):
        return [r.model_copy() for r in self.rows.values()
                if (status is None or r.status == status) and (kind is None or r.kind == kind)][:limit]

    async def list_open(self, older_than):
        return [r.model_copy() for r in self.rows.values() if r.status in OPEN_STATUSES]

    def age_all(self, seconds: int = 3600):
        for r in self.rows.values():
            r.updated_at -= timedelta(seconds=seconds)


class FakeCustomerRepo:
    """Read-only view of Supabase customers."""

    def __init__(self):
        self.rows: dict[int, dict] = {}

    def add(self, pos_customer_id: int, first_name: str, last_name: str | None = None):
        self.rows[pos_customer_id] = {
            "pos_customer_id": pos_customer_id, "first_name": first_name, "last_name": last_name,
            "customer_name": f"{first_name} {last_name or ''}".strip(), "company_name": None,
        }

    async def get(self, pos_customer_id):
        return self.rows.get(pos_customer_id)

    async def find_by_name(self, first_name, last_name):
        return [r for r in self.rows.values()
                if r["first_name"].lower() == first_name.lower()
                and (r["last_name"] or "").lower() == (last_name or "").lower()]


class FakeCustomerGateway:
    """``outcomes`` is consumed one per create/update call: an int (POS id) or an Exception to raise."""

    def __init__(self, outcomes=None):
        self.outcomes = list(outcomes or [])
        self.create_calls: list[tuple[CustomerFields, bool]] = []
        self.update_calls: list[tuple[int, CustomerFields, bool]] = []
        self.find_result: int | None = None
        self.next_id = 500

    def _next(self):
        if self.outcomes:
            out = self.outcomes.pop(0)
            if isinstance(out, Exception):
                raise out
            return out
        self.next_id += 1
        return self.next_id

    async def create(self, fields, *, commit):
        self.create_calls.append((fields, commit))
        await asyncio.sleep(0)  # a real network call yields; this lets concurrent requests interleave
        return self._next() if commit else None

    async def update(self, pos_customer_id, changes, *, commit):
        self.update_calls.append((pos_customer_id, changes, commit))
        if not commit:
            return None
        out = self._next()
        return pos_customer_id if isinstance(out, int) else out

    async def find(self, fields):
        return self.find_result


class FakeSalesGateway:
    """``create_error``: raised by create().  ``lands``: whether the sale actually reaches
    POS (lets us simulate 'reply lost but POS applied it').  ``mutate``: tamper with the
    sale POS reports back."""

    def __init__(self, pos_sale_id: int = 7441):
        self.pos_sale_id = pos_sale_id
        self.create_error: Exception | None = None
        self.lands = True
        self.readable = True
        self.mutate = None
        self.create_calls: list[tuple[ResolvedSale, bool]] = []
        self._landed: ResolvedSale | None = None

    async def create(self, sale, *, commit):
        self.create_calls.append((sale, commit))
        await asyncio.sleep(0)
        if commit and self.lands:
            self._landed = sale
        if self.create_error:
            raise self.create_error

    async def find_by_ref(self, ref, *, since):
        if self._landed is None or not self.readable or self._landed.ref != ref:
            return None
        s = self._landed
        total = s.total + (Decimal("1") if self.mutate == "total" else Decimal("0"))
        return FoundSale(
            pos_sale_id=self.pos_sale_id, pos_customer_id=s.pos_customer_id, invoice_total=total,
            lines=tuple((l.pos_item_id, l.quantity, l.unit_price) for l in s.lines), comment=s.comment_with_ref,
        )


class FakeAccountRepo:
    def __init__(self):
        self.rows: dict[UUID, str] = {}

    async def get_accounts(self, ids):
        return {i: self.rows[i] for i in ids if i in self.rows}


class FakeItemRepo:
    def __init__(self):
        self.rows: dict[int, dict] = {}

    def add(self, pos_item_id, name, selling_price, cost_price=None, inactive=False):
        self.rows[pos_item_id] = {"pos_item_id": pos_item_id, "item_name": name, "selling_price": selling_price,
                                  "cost_price": cost_price, "inactive": inactive}

    async def get_many(self, ids):
        return {i: self.rows[i] for i in ids if i in self.rows}

