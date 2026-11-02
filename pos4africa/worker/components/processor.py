from __future__ import annotations

from pos4africa.shared.models.sale import (
      ProcessedPayment, ProcessedSale, ProcessedSaleItem
)
from pos4africa.worker.components.base import BaseComponent
from pos4africa.shared.models.sale import Sale
from pos4africa.infra.local_cache import lc
from pos4africa.config.settings import settings
import hashlib, json

class Processor(BaseComponent):

      DEFAULT_CUSTOMER_ID = 23

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
            customer_id = parsed_sale.pos_customer_id

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

                        # if item.pos_item_id == 194 or item.pos_item_id == 195:
                        #       continue
                        
                        if item.pos_item_id == 83:
                              item.pos_item_id = 160
                        
                        s_item = self.get_item_by_id(lc.read_cache(settings.items_cache_key), item.pos_item_id)
                        if not s_item:
                              raise Exception(f"processor.item_parse_failed - [Can't get Item cost, Initialize the processor, pos_item_id = {item.pos_item_id}]")
                        
                        items.append(
                              ProcessedSaleItem(
                                    pos_sale_id = parsed_sale.pos_sale_id,
                                    pos_item_id  = item.pos_item_id,
                                    name        = item.name,
                                    quantity    = item.quantity,
                                    unit_price  = item.unit_price,
                                    total       = item.total,
                                    cost_price  = s_item["cost_price"]
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

                        acct_id = self.get_channel_id_by_name(lc.read_cache(settings.payment_channels_cache_key), 'STANBIC IBTC BANK' if 'STORE ACCOUNT' in payment.channel else payment.channel)
                        if acct_id is None:
                              raise ValueError("Payment Channel with {} is not found".format(payment.channel))

                        payments.append(
                              ProcessedPayment(
                                    pos_sale_id = parsed_sale.pos_sale_id,
                                    account_id  = str(acct_id),
                                    account     = 'STANBIC IBTC BANK' if 'STORE ACCOUNT' in payment.channel else payment.channel.strip().upper(),
                                    amount      = payment.amount
                              )
                        )
                  except Exception as e:
                        self.log.error(
                              "processor.payment_parse_failed",
                              error=str(e),
                              sale_id=parsed_sale.pos_sale_id
                        )

            sale_hash = self.generate_hash(json.dumps(parsed_sale.to_dict(), sort_keys=True, separators=(",", ":")))

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
                  comment          = parsed_sale.comment,
                  hash             = sale_hash,
                  items            = items,
                  payments         = payments,
            )

      # ── Helpers ─────────────────────────────────────────────────────────────
      def get_channel_id_by_name(self, channels_list: list, target_name: str) -> str | None:
            # Clean up the search string
            target_clean = target_name.strip().upper()
            
            for item in channels_list:
                  if target_clean in item['bank_name'].strip().upper():
                        return item['id']
            
            return None

      def get_item_by_id(self, items_list: list, target_id: int) -> dict:

            id_lookup = {item["pos_item_id"]: item for item in items_list}

            item = id_lookup.get(target_id)

            if not item: return None

            return item
      
      def generate_hash(self, data: str) -> str:
            return hashlib.sha256(data.encode('utf-8')).hexdigest()
 