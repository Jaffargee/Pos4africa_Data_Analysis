
from typing import Any, TypeVar
from infra.supabase_client import spb_client
import logging

T = TypeVar("T", bound="SupabaseService")

logger = logging.getLogger(__name__)

PAGE_SIZE = 1000

# We are going to use Factory Pattern to create a service class that will handle all the interactions with Supabase. This will allow us to easily switch to another database in the future if needed.
class SupabaseService:
      def __init__(self):
            self.client = spb_client

      def select(self, table_name: str, columns: list[str] | None = None, limit: int = 2000, **filters: Any) -> list[Any]:
            """Select data from a specific table in Supabase."""
            select_cols = "*" if columns is None else ",".join(columns)
            rows: list[Any] = []
            offset = 0

            while len(rows) < limit:
                  page_size = min(PAGE_SIZE, limit - len(rows))

                  if page_size <= 0:
                        break

                  query = self.client.table(table_name).select(select_cols)
                  for key, value in filters.items():
                        query = query.eq(key, value)

                  query = query.range(offset, offset + page_size - 1)
                  try:
                        response = query.execute()
                  except Exception:
                        logger.exception("Supabase select failed: table=%s filters=%s", table_name, filters)
                        raise

                  batch = response.data or []

                  if not batch:
                        break
                  
                  rows.extend(batch)

                  if len(batch) < page_size:
                        break

                  offset += page_size

            return rows

