from .supabase_client import spb_client, Client
from pos4africa.shared.utils.retry import with_retry_sync
import pandas as pd
from typing import Sequence

PAGE = 1000

class Repository:
      def __init__(self):
            self.__spb_client: Client = spb_client

      @with_retry_sync
      def __select(
            self, 
            table: str, 
            columns: str,
            filters: list[dict],
            order: dict[str, bool],
            start: int,
            end: int,
      ) -> list[dict]:
            query = self.__spb_client.table(table).select(columns or "*")
            for filter in filters:
                  query = query.filter(filter["column"], filter["operator"], filter["value"])
            query = query.order(order["column"], desc=order["desc"])
            return query.range(start, end).execute().data or []


      def select(
            self, 
            table: str, 
            columns: str = "*",
            filters: list[dict] | None = None,
            order: dict[str, bool] | None = None,
            offset: int = 0,
            limit: int | None = None,
      ) -> list[dict] | None:

            if limit is not None and limit < 0:
                  raise ValueError("limit must be >= 0")

            if limit == 0:
                  return []
            
            rows: list[dict] = []
            filters = filters or []
            order = order or {"column": "id", "desc": False}

            while limit is None or len(rows) < limit:
                  want = PAGE if limit is None else min(PAGE, limit - len(rows))
                  start = offset + len(rows)
                  end = start + want - 1
                  page = self.__select(table, columns, filters, order, start, end)
                  rows.extend(page)

                  if len(page) < want:
                        break

            return rows

      def get_sales(self) -> list[dict]:
            return self.select(
                  table='sales',
                  columns="id, pos_sale_id, pos_customer_id, customer_name, comment, invoice_total",
                  filters=[{"column": "pos_customer_id", "operator": "in", "value": "(22,23,24)"}]
            )

      def get_customers(self) -> list[dict]:
            return self.select(
                  table='customers',
                  columns="id, pos_customer_id, first_name, last_name, customer_name",
            )