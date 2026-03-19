"""
sale.py — canonical Pydantic models that flow through the entire pipeline.

Stages:
  RawSale       → output of Scraper     (everything is str | None)
  ParsedSale    → output of Parser      (typed, normalised)
  ProcessedSale → output of Processor   (relational IDs resolved, DB-ready)
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, field_validator, model_validator

class RawSaleItem(BaseModel):
      pos_sale_id: str | None = None
      pos_prd_id:  str | None = None
      name:        str | None = None
      quantity:    str | None = None
      unit_price:  str | None = None
      total:       str | None = None


class RawPayment(BaseModel):
      account: str | None = None
      amount:  str | None = None


class RawSale(BaseModel):
      pos_sale_id:             str | None = None
      invoice_datetime:        str | None = None
      salesperson:             str | None = None
      customer_name:           str | None = None
      is_anonymous_customer:   bool       = False
      invoice_total:           str | None = None
      items_sold:              str | None = None
      items_returned:          str | None = None
      change_due:              str | None = None
      comment:                 str | None = None
      items:                   list[RawSaleItem] = []
      payments:                list[RawPayment]  = []
      

class SaleItem(BaseModel):
      pos_prd_id: int | None = None
      pos_sale_id: int | None = None
      name: str | None
      quantity: int | None = None
      unit_price: Decimal
      total: Decimal

class Payment(BaseModel):
      account_id: str | None = None
      account: str | None = None
      amount: Decimal

class Sale(BaseModel):
      # IDs
      pos_sale_id: int
      
      # Datetime
      invoice_datetime: datetime
      
      # persons
      salesperson: str
      customer_name: str
      comment: str
      is_anonymous_customer: bool = False
      
      # Numbers
      invoice_total: Decimal
      change_due: Decimal
      items_net: int
      items_sold: int
      items_returned: int
      
      # Arrays
      items: list[SaleItem] = []
      payments: list[Payment] = []
      




# ── Stage 2 output: parsed & normalised ──────────────────────────────────────
class ParsedSale(BaseModel):
      """Output of Parser. Typed and normalised."""

      scrape_id: str
      node_id: str
      scraped_at: datetime

      pos_sale_id: str
      terminal_ref: str
      customer_name: str
      customer_phone: str | None = None
      amount: Decimal
      fee: Decimal = Decimal("0.00")
      payment_method: PaymentMethod
      status: SaleStatus
      transaction_date: datetime
      narration: str | None = None

      @field_validator("amount", "fee", mode="before")
      @classmethod
      def coerce_decimal(cls, v: object) -> Decimal:
            if isinstance(v, Decimal):
                  return v
            cleaned = str(v).replace(",", "").replace("₦", "").strip()
            return Decimal(cleaned)

      @field_validator("customer_name", mode="before")
      @classmethod
      def strip_name(cls, v: object) -> str:
            return str(v).strip().title()


# ── Stage 3 output: processor-ready, IDs resolved ────────────────────────────

class ProcessedSale(BaseModel):
      """Output of Processor. Matches the Supabase `sales` table schema exactly."""

      id: UUID = Field(default_factory=uuid4)
      pos_sale_id: str                  # natural key from POS system
      terminal_id: UUID                 # FK → terminals
      customer_id: UUID | None = None   # FK → customers (nullable)
      amount: Decimal
      fee: Decimal
      payment_method: PaymentMethod
      status: SaleStatus
      transaction_date: datetime
      narration: str | None = None
      node_id: str
      scraped_at: datetime
      created_at: datetime = Field(default_factory=datetime.utcnow)

      def to_db_dict(self) -> dict:
            """Serialise to a dict safe for Supabase upsert."""
            data = self.model_dump(mode="json")
            # Supabase expects string UUIDs
            for key in ("id", "terminal_id", "customer_id"):
                  if data[key] is not None:
                        data[key] = str(data[key])
                        data["amount"] = str(self.amount)
                        data["fee"] = str(self.fee)
            return data