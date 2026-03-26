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
from pos4africa.worker.components import Sync
from .search_nomalisation import search_customer

def _normalise_name(name: str) -> str: return name.lower().strip()

class LongTermMemory:
      
      def __init__(self, redis: RedisClient) -> None:
            self._redis: RedisClient = redis
            self._refresh_task: asyncio.Task | None = None
            self._CUSTOMER_KEY = settings.redis_customers_id

      # ── Lifecycle ─────────────────────────────────────────────────────────────

      async def warm_up(self) -> None:
            await self._sync_customers()
            self._refresh_task = asyncio.create_task(self._refresh_loop())

      async def shutdown(self) -> None:
            if self._refresh_task:
                  self._refresh_task.cancel()

      async def _refresh_loop(self) -> None:
            while True:
                  await asyncio.sleep(settings.redis_long_term_refresh_interval)
                  try:
                        await self._sync_customers()
                  except Exception as exc:
                        print(exc)

      async def _sync_customers(self) -> None:
            result = Sync.fetch_customers()
            if result:
                  mapping = {
                        _normalise_name(row.name): row.pos_customer_id for row in result if row.name
                  }
                  if mapping:
                        await self._redis.hset(self._CUSTOMER_KEY, mapping=mapping)  # type: ignore[arg-type]

      # ── Customer lookup ───────────────────────────────────────────────────────

      async def get_customer_id_by_name(self, name: str) -> int | None:
            normalised_name = _normalise_name(name)
            
            customer_id = await self._redis.hget(self._CUSTOMER_KEY, normalised_name)
            if customer_id:
                  return int(customer_id)
            
            customers = await self.get_customers()
            best_match = search_customer(normalised_name, list(customers.keys()))
            
            if not best_match:
                  return None
            
            return int(await self._redis.hget(self._CUSTOMER_KEY, best_match))

      async def get_customers(self) -> dict | None:
            return await self._redis.hgetall(self._CUSTOMER_KEY) or None

      # ── Deduplication ─────────────────────────────────────────────────────────

      async def is_duplicate(self, fingerprint: str) -> bool:
            return bool(await self._redis.sismember(settings.redis_jobs_dedup_key, fingerprint))

      async def mark_seen(self, fingerprint: str) -> None:
            await self._redis.sadd(settings.redis_jobs_dedup_key, fingerprint)