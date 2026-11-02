from pos4africa.infra.supabase_client import spb_client
from pos4africa.shared.utils.retry import with_retry_sync
from pos4africa.infra.local_cache import lc

import asyncio

class LocalStorage:
      def __init__(self) -> LocalStorage:
            self.__cache_table: list[dict[str, str]] = []
      
      def __add_table2cache(self, __table: str, __cache_key: str, __columns: str) -> None:
            self.__cache_table.append({ "table": __table, "key": __cache_key, "columns": __columns })

      def add(self, __table: str, __cache_key: str, __columns: str ) -> LocalStorage:
            self.__add_table2cache(__table, __cache_key, __columns)
            return self

      async def init_localstorage(self) -> None:
            if not self.__cache_table: return None

            __cache_miss: list[dict[str, str]] = []

            for ct in self.__cache_table:
                  if "key" in ct and not self.__is_cache_hit(ct["key"]):
                        __cache_miss.append(ct)

            tasks = []
            for cache_t in __cache_miss:

                  if "table" not in cache_t or "key" not in cache_t or "columns" not in cache_t:
                        continue

                  tasks.append(
                        self.__store_data_to_local_storage(cache_t["key"], cache_t["table"], cache_t["columns"])
                  )

            await asyncio.gather(*tasks)

      @with_retry_sync
      def __fetch_data(self, table: str, columns: str):
            columns = columns if columns else "*"
            response = spb_client.table(table).select(columns).execute()
            if not response or response.data is None:
                  return []

            return response.data


      async def __store_data_to_local_storage(self, cache_key: str, table: str, columns: str):
            data = await asyncio.to_thread(self.__fetch_data, table, columns)
            if not data:
                  return False
            return lc.write_cache_atomic(cache_key, data)

      def __is_cache_hit(self, key: str) -> bool:
            cache_hit = lc.read_cache(key)
            return bool(cache_hit)