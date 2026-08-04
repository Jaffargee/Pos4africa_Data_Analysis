"""
redis_client.py — async Redis client (singleton).
"""

from __future__ import annotations

from typing import TypeAlias
import redis.asyncio as aioredis
from pos4africa.config.settings import settings
RedisClient: TypeAlias = aioredis.Redis

_client: RedisClient | None = None

async def get_redis() -> RedisClient:
      global _client
      if _client is None:
            _client = await aioredis.from_url(
                  settings.redis_url,
                  encoding="utf-8",
                  decode_responses=True,
            )
      return _client


async def close_redis() -> None:
      global _client
      if _client:
            await _client.aclose()
            _client = None