"""
batch_writer.py

Writes the Excel-ingested sales directly to Supabase tables.
This bypasses the legacy `insert_sale` RPC, which is out of sync with the
current runtime payload.
"""

from __future__ import annotations

from typing import Any

from pos4africa.config.settings import settings
from pos4africa.infra.supabase_client import spb_client
from pos4africa.shared.utils.logger import get_logger
from pos4africa.shared.utils.retry import with_retry_async

log = get_logger(__name__)


class BatchWriter:
      def __init__(self) -> None:
            self._sales_table = settings.supabase_table_sales
            self._sale_items_table = "sale_items"
            self._sale_payments_table = "payments"
            self._batch_size = settings.supabase_batch_size

      async def write(self, records: list[dict[str, Any]]) -> int:
            if not records:
                  return 0

            total = 0
            for i in range(0, len(records), self._batch_size):
                  chunk = records[i : i + self._batch_size]
                  await self._write_chunk(chunk)
                  total += len(chunk)

                  log.info(
                        "batch_writer.chunk_written",
                        chunk_size=len(chunk),
                        total_written=total,
                  )

            return total

      @with_retry_async
      async def _write_chunk(self, chunk: list[dict[str, Any]]) -> None:
            sales_rows = [self._build_sales_row(record) for record in chunk]
            sale_ids = [record["pos_sale_id"] for record in chunk]
            item_rows = [
                  self._build_item_row(item, record["pos_sale_id"])
                  for record in chunk
                  for item in record.get("items", [])
            ]
            payment_rows = [
                  self._build_payment_row(payment, record["pos_sale_id"])
                  for record in chunk
                  for payment in record.get("payments", [])
            ]

            sales_result = spb_client.table(self._sales_table).upsert(
                  sales_rows,
                  on_conflict="pos_sale_id",
              ).execute()
            self._raise_on_error(sales_result, self._sales_table)

            delete_items_result = (
                  spb_client.table(self._sale_items_table)
                  .delete()
                  .in_("pos_sale_id", sale_ids)
                  .execute()
            )
            self._raise_on_error(delete_items_result, self._sale_items_table)

            delete_payments_result = (
                  spb_client.table(self._sale_payments_table)
                  .delete()
                  .in_("pos_sale_id", sale_ids)
                  .execute()
            )
            self._raise_on_error(delete_payments_result, self._sale_payments_table)

            if item_rows:
                  item_result = spb_client.table(self._sale_items_table).insert(item_rows).execute()
                  self._raise_on_error(item_result, self._sale_items_table)

            if payment_rows:
                  payment_result = spb_client.table(self._sale_payments_table).insert(payment_rows).execute()
                  self._raise_on_error(payment_result, self._sale_payments_table)

      def _build_sales_row(self, record: dict[str, Any]) -> dict[str, Any]:
            return {
                  "pos_sale_id": record["pos_sale_id"],
                  "invoice_total": record["invoice_total"],
                  "pos_customer_id": record.get("pos_customer_id"),
                  "customer_name": record.get("customer_name"),
                  "salesperson": record.get("salesperson"),
                  "invoice_datetime": record["invoice_datetime"],
                  "comment": record.get("comment"),
                  "is_anonymous_customer": record.get("is_anonymous_customer", False),
                  "items_net": record.get("items_net", 0),
                  "items_sold": record.get("items_sold", 0),
                  "items_returned": record.get("items_returned", 0),
            }

      def _build_item_row(self, item: dict[str, Any], sale_id: int) -> dict[str, Any]:
            return {
                  "pos_sale_id": sale_id,
                  "pos_item_id": item.get("pos_item_id"),
                  "name": item.get("name"),
                  "quantity": item.get("quantity", 0),
                  "unit_price": item.get("unit_price", 0),
                  "total": item.get("total", 0),
            }

      def _build_payment_row(self, payment: dict[str, Any], sale_id: int) -> dict[str, Any]:
            return {
                  "pos_sale_id": sale_id,
                  "account": payment.get("account"),
                  "account_id": payment.get("account_id"),
                  "amount": payment.get("amount", 0),
            }

      def _raise_on_error(self, result: Any, table: str) -> None:
            if hasattr(result, "error") and result.error:
                  raise RuntimeError(f"Supabase write failed for '{table}': {result.error}")
