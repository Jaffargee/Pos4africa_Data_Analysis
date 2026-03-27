from __future__ import annotations
 
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from dateutil import parser as dateutil_parser

from pos4africa.shared.models.sale import RawSale, RawPayment, RawSaleItem, Sale, Payment, SaleItem
from pos4africa.worker.components.base import BaseComponent
from pos4africa.manager.memory.store import MemoryStore

_DATE_FORMATS = [
      "%m/%d/%Y %I:%M %p",   # 03/16/2026 05:53 pm  ← pos4africa format
      "%m/%d/%Y %H:%M",
      "%Y-%m-%dT%H:%M:%S",
      "%d/%m/%Y %H:%M:%S",
]

class Parser(BaseComponent):
      def __init__(self, node_id: str, memory: MemoryStore):
            super().__init__(node_id, memory)
      
      async def run(self, raw_sale: RawSale) -> Sale | None:
            return self._parse(raw_sale)
      
      def _parse(self, raw: RawSale) -> Sale:
            customer_name = self._clean_str(raw.customer_name) or "Unknown Customer"

            return Sale(
                  pos_sale_id      = self._parse_int(self._require(raw.pos_sale_id, "pos_sale_id"), "pos_sale_id"),
                  invoice_datetime = self._parse_datetime(raw.invoice_datetime),
                  salesperson      = self._clean_str(self._require(raw.salesperson, "salesperson")),

                  customer_name          = customer_name,
                  is_anonymous_customer  = raw.is_anonymous_customer,

                  invoice_total    = self._parse_decimal(raw.invoice_total, "invoice_total"),
                  change_due       = self._parse_decimal(raw.change_due, "change_due", default=Decimal("0.00")),

                  items_sold     = self._parse_int(raw.items_sold, "items_sold"),
                  items_returned = self._parse_int(raw.items_returned, "items_returned", default=0),
                  items_net      = self._parse_int(raw.items_sold, "items_sold") - self._parse_int(raw.items_returned, "items_returned", default=0),
                  
                  comment  = self._clean_str(raw.comment),
                  items    = [self._parse_item(i) for i in raw.items],
                  payments = [self._parse_payment(p) for p in raw.payments],
            )
      
      # ── Sub-model parsers ─────────────────────────────────────────────────────
      
      def _parse_item(self, raw: RawSaleItem) -> SaleItem:
            return SaleItem(
                  pos_prd_id    = self._parse_int(raw.pos_prd_id, "rawsale.pos_prd_id"),
                  pos_sale_id    = self._parse_int(raw.pos_sale_id, "rawsale.pos_sale_id"),
                  name       = self._clean_str(raw.name),
                  unit_price = self._parse_decimal(raw.unit_price, "unit_price"),
                  quantity   = self._parse_int(raw.quantity, "quantity"),
                  total      = self._parse_decimal(raw.total, "total"),
            )
      
      def _parse_payment(self, raw: RawPayment) -> Payment:
            return Payment(
                  channel = raw.channel,
                  amount = self._parse_decimal(raw.amount, "payment.amount"),
            )
      
      # ── Field normalisers ─────────────────────────────────────────────────────

      def _require(self, value: str | None, field: str) -> str:
            if not value or not value.strip():
                  raise ValueError(f"Required field '{field}' is empty")
            return value.strip()
 
      def _clean_str(self, value: str | None) -> str | None:
            if not value:
                  return None
            cleaned = re.sub(r"\s+", " ", value).strip()
            return cleaned or None

      def _parse_decimal(self, value: str | None, field: str, default: Decimal | None = None):
            if not value:
                  if default is not None:
                        return default
                  raise ValueError(f"Missing decimal field '{field}'")
            
            cleaned = re.sub(r"[₦, \s]", "", value)

            try:
                  return Decimal(cleaned)
            except InvalidOperation:
                  raise ValueError(f"Cannot parse '{value}' as decimal for '{field}'")

      def _parse_decimal_optional(self, value: str | None) -> Decimal | None:
            if not value:
                  return None
            try:
                  return self._parse_decimal(value, "_optional")
            except ValueError:
                  return None
      
      def _parse_int(self, value: str | None, field: str, default: int | None = None) -> int:
            if not value:
                  if default is not None:
                        return default
                  raise ValueError(f"Missing int field '{field}'")
            try:
                  return int(float(value.strip()))
            except (ValueError, AttributeError):
                  raise ValueError(f"Cannot parse '{value}' as int for '{field}'")
      
      def _parse_datetime(self, value: str | None) -> datetime:
            if not value:
                  raise ValueError("Missing invoice_datetime")
            for fmt in _DATE_FORMATS:
                  try:
                        return datetime.strptime(value.strip(), fmt)
                  except ValueError:
                        continue
            try:
                  return dateutil_parser.parse(value.strip())
            except Exception:
                  raise ValueError(f"Cannot parse datetime: '{value}'")
      