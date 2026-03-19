from __future__ import annotations
 
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
 
from pos4africa.shared.models.sale import RawSale, RawPayment, RawSaleItem, Sale, Payment, SaleItem

_DATE_FORMATS = [
      "%m/%d/%Y %I:%M %p",   # 03/16/2026 05:53 pm  ← pos4africa format
      "%m/%d/%Y %H:%M",
      "%Y-%m-%dT%H:%M:%S",
      "%d/%m/%Y %H:%M:%S",
]

class Parser:
      def __init__(self):
            pass
      
      def run(self) -> Sale | None:
            pass
      
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

      