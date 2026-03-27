from __future__ import annotations

from pos4africa.shared.models.sale import (
      ProcessedPayment, ProcessedSale, ProcessedSaleItem
)
from pos4africa.worker.components.base import BaseComponent
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.models.sale import Sale


class Processor(BaseComponent):

      DEFAULT_CUSTOMER_ID = 52

      def __init__(self, node_id: str, memory: MemoryStore) -> None:
            super().__init__(node_id=node_id, memory=memory)
            
            self._DEFAULT_DB_ACCOUNTS_NAME = [
                  'ACCESS BANK', 'STANBIC IBTC BANK',
                  
            ]

      async def run(self, parsed_sale: Sale | None) -> ProcessedSale | None:

            if not parsed_sale:
                  self.log.warning(
                        "processor.parsed_missing",
                        node_id=self.node_id
                  )
                  return None

            try:
                  result = await self._process(parsed_sale)
                  self._on_success()
                  return result

            except Exception as e:
                  self._on_error(e)
                  return None

      async def _process(self, parsed_sale: Sale) -> ProcessedSale:

            # ── Resolve customer ────────────────────────────────────────────────
            customer_id = await self._resolve_customer_id(parsed_sale.customer_name)

            if not customer_id:
                  self.log.warning(
                        "processor.customer_default_used",
                        customer_name=parsed_sale.customer_name,
                        sale_id=parsed_sale.pos_sale_id
                  )
                  customer_id = self.DEFAULT_CUSTOMER_ID

            # ── Process items ──────────────────────────────────────────────────
            items: list[ProcessedSaleItem] = []

            for item in parsed_sale.items or []:
                  try:
                        items.append(
                              ProcessedSaleItem(
                                    pos_sale_id = parsed_sale.pos_sale_id,
                                    pos_prd_id  = item.pos_prd_id,
                                    name        = item.name,
                                    quantity    = self._parse_int(item.quantity, "quantity", 0),
                                    unit_price  = self._parse_float(item.unit_price, "unit_price", 0.0),
                                    total       = self._parse_float(item.total, "total", 0.0),
                              )
                        )
                  except Exception as e:
                        self.log.error(
                              "processor.item_parse_failed",
                              error=str(e),
                              sale_id=parsed_sale.pos_sale_id
                        )

            # ── Process payments ───────────────────────────────────────────────
            payments: list[ProcessedPayment] = []

            for payment in parsed_sale.payments or []:
                  try:
                        account_id = await self._resolve_account_id(payment.channel)
                        
                        payments.append(
                              ProcessedPayment(
                                    pos_sale_id = parsed_sale.pos_sale_id,
                                    account_id  = str(account_id),
                                    account     = payment.channel,
                                    amount      = self._parse_float(payment.amount, "payment_amount", 0.0),
                              )
                        )
                  except Exception as e:
                        self.log.error(
                              "processor.payment_parse_failed",
                              error=str(e),
                              sale_id=parsed_sale.pos_sale_id
                        )

            # ── Build final processed sale ─────────────────────────────────────
            return ProcessedSale(
                  pos_sale_id      = parsed_sale.pos_sale_id,
                  invoice_datetime = parsed_sale.invoice_datetime,
                  salesperson      = parsed_sale.salesperson,
                  customer_id      = customer_id,
                  account_id       = account_id,
                  invoice_total    = self._parse_float(parsed_sale.invoice_total, "invoice_total", 0.0),
                  items_sold       = self._parse_int(parsed_sale.items_sold, "items_sold", 0),
                  items_returned   = self._parse_int(parsed_sale.items_returned, "items_returned", 0),
                  change_due       = self._parse_float(parsed_sale.change_due, "change_due", 0.0),
                  comment          = parsed_sale.comment,
                  items            = items,
                  payments         = payments,
            )

      # ── Helpers ──────────────────────────────────────────────────────────────

      async def _resolve_customer_id(self, name: str | None) -> int | None:
            if not name:
                  return None
            return await self.memory.ltm.get_customer_id_by_name(name)

      async def _resolve_account_id(self, name: str | None) -> str | None:
            if not name:
                  return None
            return await self.memory.ltm.get_accounts_id_by_name(name)

      def _parse_int(self, value: str | None, field: str, default: int | None = None) -> int:
            if not value:
                  if default is not None:
                        return default
                  raise ValueError(f"Missing int field '{field}'")

            try:
                  return int(float(value.strip()))
            except (ValueError, AttributeError):
                  raise ValueError(f"Cannot parse '{value}' as int for '{field}'")

      def _parse_float(self, value: str | None, field: str, default: float | None = None) -> float:
            if not value:
                  if default is not None:
                        return default
                  raise ValueError(f"Missing float field '{field}'")

            try:
                  return float(value.replace(",", "").strip())
            except (ValueError, AttributeError):
                  raise ValueError(f"Cannot parse '{value}' as float for '{field}'")