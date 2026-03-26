# tester.py
from pos4africa.worker.components import Sync
from pos4africa.manager.memory.long_term import LongTermMemory
from pos4africa.infra.redis_client import get_redis
from pos4africa.manager.job_producer import JobProducer
from pos4africa.config import settings
import json, asyncio

async def main():
      # job_p = JobProducer((await get_redis()))
      # await job_p.produce(list(range(1, 16000)))
      # ltm = LongTermMemory(redis=(await get_redis()))
      # await ltm.warm_up()
      # redis = await get_redis()
      # ctm_id = await ltm.get_customer_id_by_name('MALAM ADAmu')
      # print(ctm_id, type(ctm_id))
      # print((await ltm.get_customers())['ibrahim bb shop'])
      
      # redis = await get_redis()
            
      # print(await redis.brpop(settings.redis_queue_key))
      # print(await redis.brpop(settings.redis_queue_key))
      # print(await redis.brpop(settings.redis_queue_key))
      # print(await redis.brpop(settings.redis_queue_key))
      # print(await redis.brpop(settings.redis_queue_key))
      pass
      
asyncio.run(main())

# sale_ids = list(range(16000))
# for i in range(1, 16000, 500):
#       batch = sale_ids[i:i + 500]
#       # print(batch)
#       # print('========================\n\n')
      
#       for sale_id in batch:
#             for idx, sale_id in enumerate(batch):
#                   print(results[idx * 2])
