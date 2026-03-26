from pos4africa.infra.redis_client import get_redis, RedisClient
from pos4africa.config import settings

class JobProducer:
      
      def __init__(self, redis: RedisClient):
            self.redis = redis
            
            
      async def produce(self, sales_ids: list[int]):
            total = len(sales_ids)
            pushed = 0
            
            for i in range(0, total, settings.worker_batch_size):
                  batch = sales_ids[i:i + settings.worker_batch_size]
                  pipe = self.redis.pipeline()
                  
                  for sale_id in batch:
                        pipe.setnx(f"{settings.redis_jobs_dedup_key}:{sale_id}", sale_id)
                        pipe.expire(f"{settings.redis_jobs_dedup_key}:{sale_id}", 86400)

                  results = await pipe.execute()
                  
                  pipe = self.redis.pipeline()
                  
                  for idx, sale_id in enumerate(batch):
                        if results[idx * 2]:
                              pipe.lpush(settings.redis_queue_key, sale_id)
                              pushed += 1

                  await pipe.execute()
                  
                  
                  print(f"[Producer] Batch {i} → pushed {pushed}")

            print(f"[Producer] DONE: {pushed}/{total}")