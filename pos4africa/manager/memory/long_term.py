"""
long_term.py — LongTermMemory

Redis-backed reference data shared across nodes within a Host Manager.
Warmed up from Supabase on Host start, refreshed periodically.

Stores:
customers  → hash keyed by normalised name
dedup      → SET of sale_id fingerprints (shared across all nodes)
"""

from __future__ import annotations

import asyncio
import orjson
from pos4africa.config.settings import settings
from pos4africa.infra.redis_client import RedisClient
from .search_nomalisation import search_
from pos4africa.shared.utils.logger import get_logger

def _normalise_name(name: str) -> str: return name.lower().strip()

class LongTermMemory:
      
      def __init__(self, redis: RedisClient) -> None:
            self._redis: RedisClient = redis
            self._refresh_task: asyncio.Task | None = None
            self._CUSTOMER_KEY = settings.redis_customers_id
            self.log = get_logger(__name__)
            
            
      async def redis_set(self, key: str, value: dict, **kwargs) -> None:
            await self._redis.set(key, orjson.dumps(value), **kwargs)
            
      async def redis_get(self, key: str) -> dict | None:
            value = await self._redis.get(key)
            if value:
                  return orjson.loads(value)
            return None

      # ── Lifecycle ─────────────────────────────────────────────────────────────

      async def warm_up(self) -> None:
            await self._sync_customers()
            await self._sync_accounts()
            self._refresh_task = asyncio.create_task(self._refresh_loop())

      async def shutdown(self) -> None:
            if self._refresh_task:
                  self._refresh_task.cancel()

      async def _refresh_loop(self) -> None:
            while True:
                  await asyncio.sleep(settings.redis_long_term_refresh_interval)
                  try:
                        await self._sync_customers()
                        await self._sync_accounts()
                  except Exception as exc:
                        self.log("Failed to sync customers & accounts with an Exception", exc=exc)

      async def _sync_customers(self) -> None:
            from pos4africa.worker.components import Sync 
            result = Sync.fetch_customers()
            if result:
                  mapping = {
                        _normalise_name(row.name): row.pos_customer_id for row in result if row.name
                  }
                  if mapping:
                        await self._redis.hset(self._CUSTOMER_KEY, mapping=mapping)  # type: ignore[arg-type]
                        
      async def _sync_accounts(self) -> None:
            from pos4africa.worker.components import Sync 
            result = Sync.fetch_accounts()
            if result:
                  mapping = {
                        _normalise_name(row.account_bank): str(row.id) for row in result if row.account_bank
                  }
                  
                  if mapping:
                        await self._redis.hset(settings.redis_accounts_id, mapping=mapping)

      # ── Customer lookup ───────────────────────────────────────────────────────

      async def get_customer_id_by_name(self, name: str) -> int | None:
            normalised_name = _normalise_name(name)
            return await self.get_id_by_name(name, self._CUSTOMER_KEY, self.get_customers)

      async def get_accounts_id_by_name(self, name: str) -> int | None:
            normalised_name = _normalise_name(name)
            return await self.get_id_by_name(name, settings.redis_accounts_id, self.get_accounts)
      
      async def get_id_by_name(self, name: str, redis_key: str, fuzz_list_callback: any) -> str | None:
            normalised_name = _normalise_name(name)
            
            _id = await self._redis.hget(redis_key, normalised_name)
            
            if _id:
                  return _id
            
            _fuzz_list = await fuzz_list_callback()
            best_match = search_(normalised_name, list(_fuzz_list.keys()))
            
            if not best_match:
                  return None
            
            return await self._redis.hget(redis_key, best_match)
            
      async def get_customers(self) -> dict | None:
            return await self._redis.hgetall(self._CUSTOMER_KEY) or None
            
      async def get_accounts(self) -> dict | None:
            return await self._redis.hgetall(settings.redis_accounts_id) or None

      # ── Deduplication ─────────────────────────────────────────────────────────

      async def is_duplicate(self, fingerprint: str) -> bool:
            return bool(await self._redis.sismember(settings.redis_jobs_dedup_key, fingerprint))

      async def mark_seen(self, fingerprint: str) -> None:
            await self._redis.sadd(settings.redis_jobs_dedup_key, fingerprint)