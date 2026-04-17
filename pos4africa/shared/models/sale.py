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
      pos_item_id:  str | None = None
      name:        str | None = None
      quantity:    str | None = None
      unit_price:  str | None = None
      total:       str | None = None

class RawPayment(BaseModel):
      channel: str | None = None
      amount:  str | None = None


class RawSale(BaseModel):
      pos_sale_id:             str | None = None
      pos_customer_id:         str | None = None
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
      pos_item_id: int | None = None
      pos_sale_id: int | None = None
      name: str | None
      quantity: int | None = None
      unit_price: Decimal
      total: Decimal

class Payment(BaseModel):
      channel: str | None = None
      amount: Decimal

class Sale(BaseModel):
      # IDs
      pos_sale_id: int
      pos_customer_id: int | None = None

      # Datetime
      invoice_datetime: datetime

      # persons
      salesperson: str
      customer_name: str
      comment: str | None = None
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
      

# ── Stage 3 output: processor-ready, IDs resolved ────────────────────────────

class ProcessedPayment(BaseModel):
      id: UUID | None = None
      pos_sale_id: int
      account_id: UUID
      account: str | None = None
      amount: Decimal


class ProcessedSaleItem(BaseModel):
      id: UUID | None = None
      pos_item_id: int | None = None
      pos_sale_id: int | None = None
      name: str | None
      quantity: int | None = None
      unit_price: Decimal
      total: Decimal

class ProcessedSale(BaseModel):
      """Output of Processor. Matches the Supabase `sales` table schema exactly."""
      id: UUID | None = None
      # IDs
      pos_sale_id: int
      pos_customer_id: int
      
      # Datetime
      invoice_datetime: datetime
      scraped_at: datetime | None = None

      # persons
      salesperson: str
      customer_name: str
      comment: str | None = None
      is_anonymous_customer: bool = False

      # Numbers
      invoice_total: Decimal
      change_due: Decimal
      items_net: int
      items_sold: int
      items_returned: int
      
      # Arrays
      items: list[ProcessedSaleItem] = []
      payments: list[ProcessedPayment] = []

      def to_db_dict(self) -> dict:
            """Serialise to the live Supabase table schema."""
            return {
                  "pos_sale_id": self.pos_sale_id,
                  "pos_customer_id": self.pos_customer_id,
                  "invoice_total": float(self.invoice_total),
                  "customer_name": self.customer_name,
                  "salesperson": self.salesperson,
                  "invoice_datetime": self.invoice_datetime.isoformat(),
                  "comment": self.comment,
                  "is_anonymous_customer": self.is_anonymous_customer,
                  "items_net": self.items_net,
                  "items_sold": self.items_sold,
                  "items_returned": self.items_returned,
                  "items": [
                        {
                              "pos_sale_id": item.pos_sale_id,
                              "pos_item_id": item.pos_item_id,
                              "name": item.name,
                              "quantity": item.quantity,
                              "unit_price": float(item.unit_price),
                              "total": float(item.total),
                        }
                        for item in self.items
                  ],
                  "payments": [
                        {
                              "pos_sale_id": payment.pos_sale_id,
                              "account_id": str(payment.account_id),
                              "account": payment.account,
                              "amount": float(payment.amount),
                        }
                        for payment in self.payments
                  ],
            }
