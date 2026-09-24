from redis_service import RedisService
from supabase_service import SupabaseService
from config import Config

class DBObject:
      def __init__(self) -> None:
            self.__redis = RedisService()
            self.__supabase = SupabaseService()

      def __build_cache_key(self, table_name: str, **filters) -> str:
            if not filters:
                  return table_name

            filter_parts = ",".join(f"{k}={v}" for k, v in sorted(filters.items()))
            return f"{table_name}:{filter_parts}"

      def get(self, name: str, columns: list[str] | None = None, limit: int = 2000, **filters):
            policy = Config.cache_policy_for(name)

            if not policy:
                  return self.__supabase.select(name, columns=columns, limit=limit, **filters)

            key = self.__build_cache_key(name, **filters)

            cached = self.__redis.get(key)
            if cached is not None:
                  return cached

            data = self.__supabase.select(name, columns=columns, limit=limit, **filters)
            self.__redis.store(key, data, ttl_seconds=policy["ttl_seconds"])
            return data

      def invalidate(self, name: str, **filters):
            if Config.cache_policy_for(name) is None:
                  return

            self.__redis.delete(self.__build_cache_key(name, **filters))

      def flush_db(self):
            self.__redis.flush_db()