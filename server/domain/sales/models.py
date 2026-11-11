from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

TWO_PLACES = Decimal("0.01")
PAYMENT_TOLERANCE = Decimal("1")  # naira; same tolerance the old create_pos_sale RPC used


def money(value: Decimal) -> Decimal:
    return Decimal(value).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)


# --- request -----------------------------------------------------------------
class SaleLineRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pos_item_id: int
    quantity: Decimal = Field(gt=0, max_digits=12, decimal_places=4)
    unit_price: Decimal | None = Field(default=None, ge=0, max_digits=14, decimal_places=2)


class PaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account_id: UUID
    amount: Decimal = Field(gt=0, max_digits=14, decimal_places=2)


class CreateSaleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    pos_customer_id: int | None = None  # None -> the walk-in bucket
    lines: list[SaleLineRequest] = Field(min_length=1, max_length=100)
    payments: list[PaymentRequest] = Field(min_length=1, max_length=10)
    comment: str | None = Field(default=None, max_length=200)


# --- resolved (what we persist in the write log and send to POS) ----------------
class ResolvedLine(BaseModel):
    pos_item_id: int
    name: str
    quantity: Decimal
    unit_price: Decimal
    total: Decimal
    cost_price: Decimal | None = None


class ResolvedPayment(BaseModel):
    account_id: UUID
    account: str  # POS payment-type label == accounts.name
    amount: Decimal


class ResolvedSale(BaseModel):
    ref: str
    pos_customer_id: int
    customer_name: str
    salesperson: str
    comment: str | None
    comment_with_ref: str
    lines: list[ResolvedLine]
    payments: list[ResolvedPayment]
    total: Decimal
    fingerprint: str


# --- fingerprint ---------------------------------------------------------------
def _norm(d: Decimal) -> str:
    return format(Decimal(d).normalize(), "f")


def compute_fingerprint(
    pos_customer_id: int,
    lines: Iterable[tuple[int, Decimal, Decimal]],
    total: Decimal,
    ref: str,
) -> str:
    """Stable hash of what a sale *is*: who, which items/qty/price, how much, which ref.

    Computed from our intent before sending, and again from what POS reports back.
    Equal fingerprints = the sale in POS is the sale we meant to create.
    """
    canonical = {
        "customer": pos_customer_id,
        "lines": sorted([item, _norm(qty), _norm(price)] for item, qty, price in lines),
        "total": _norm(money(total)),
        "ref": ref,
    }
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


@dataclass(frozen=True)
class FoundSale:
    """A sale as read back from POS."""

    pos_sale_id: int
    pos_customer_id: int
    invoice_total: Decimal
    lines: tuple[tuple[int, Decimal, Decimal], ...]  # (pos_item_id, quantity, unit_price)
    comment: str | None = None
    invoice_datetime: datetime | None = None

    def fingerprint(self, ref: str) -> str:
        return compute_fingerprint(self.pos_customer_id, self.lines, self.invoice_total, ref)
