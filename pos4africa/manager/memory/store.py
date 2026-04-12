
from __future__ import annotations

from .long_term import LongTermMemory
from pos4africa.infra.redis_client import get_redis, RedisClient

class MemoryStore:
      def __init__(self, node_id: str, redis: RedisClient) -> None:
            self.node_id = node_id
            self.ltm = LongTermMemory(redis=redis)
            
      async def initialise(self) -> None:
            await self.ltm.warm_up()
            
      async def close(self) -> None:
            pass