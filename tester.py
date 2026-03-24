# tester.py
from pos4africa.worker.components import Sync
from pos4africa.worker.memory.long_term import LongTermMemory
from pos4africa.infra.redis_client import get_redis
from pos4africa.config import settings
import json, asyncio

async def main():
      ltm = LongTermMemory(redis=(await get_redis()))
      await ltm.warm_up()
      redis = await get_redis()
      customers = await redis.hgetall(settings.redis_customers_key)
      print(customers)
      
asyncio.run(main())