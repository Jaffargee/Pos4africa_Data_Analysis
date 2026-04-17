from __future__ import annotations

from pos4africa.shared.models.sale import (
      ProcessedPayment, ProcessedSale, ProcessedSaleItem
)
from pos4africa.worker.components.base import BaseComponent
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.models.sale import Sale
from pos4africa.manager.memory.search_nomalisation import search_
import re

class Processor(BaseComponent):

      DEFAULT_CUSTOMER_ID = 23

      def __init__(self, node_id: str, memory: MemoryStore) -> None:
            super().__init__(node_id=node_id, memory=memory)
            
            self._DEFAULT_DB_ACCOUNTS_NAME = [
                  'ACCESS BANK', 'STANBIC IBTC BANK',
                  'MONIEPOINT MFB', 'CASH PAYMENT'
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
                                    pos_item_id  = item.pos_item_id,
                                    name        = item.name,
                                    quantity    = item.quantity,
                                    unit_price  = item.unit_price,
                                    total       = item.total
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
                        
                        best_match = search_(payment.channel, self._DEFAULT_DB_ACCOUNTS_NAME)
                        account_id = await self._resolve_account_id('STANBIC IBTC BANK' if 'STORE ACCOUNT' in payment.channel else best_match)
                        
                        payments.append(
                              ProcessedPayment(
                                    pos_sale_id = parsed_sale.pos_sale_id,
                                    account_id  = str(account_id),
                                    account     = 'STANBIC IBTC BANK' if 'STORE ACCOUNT' in payment.channel else best_match,
                                    amount      = payment.amount
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
                  pos_customer_id  = customer_id,
                  invoice_datetime = parsed_sale.invoice_datetime,
                  salesperson      = parsed_sale.salesperson,
                  customer_name    = parsed_sale.customer_name,
                  invoice_total    = parsed_sale.invoice_total,
                  items_net        = parsed_sale.items_net,
                  items_sold       = parsed_sale.items_sold,
                  items_returned   = parsed_sale.items_returned,
                  change_due       = parsed_sale.change_due,
                  comment          = parsed_sale.comment,
                  is_anonymous_customer = parsed_sale.is_anonymous_customer,
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
