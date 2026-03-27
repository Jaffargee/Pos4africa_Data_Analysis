# tester.py
from pos4africa.worker.components import Sync
from pos4africa.manager.memory.long_term import LongTermMemory
from pos4africa.infra.redis_client import get_redis
from pos4africa.manager.job_producer import JobProducer
from pos4africa.config import settings
from pos4africa.worker.components.scraper import Scraper
from pos4africa.worker.components.parser import Parser
from pos4africa.worker.components.processor import Processor
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.models.sale import Sale
import asyncio, json

async def main():
      redis = await get_redis()
      
      # job_p = JobProducer(redis)
      # await job_p.produce(list(range(1, 16000)))
      # print(await redis.brpop(settings.redis_queue_key))

      
      with open("./test/fixtures/receipt.html", "r", encoding="utf-8") as r:
            html_content = r.read()
            
            node_id = '12345'
            store = MemoryStore(node_id, redis)
            
            scraper = Scraper(node_id, store, '5555', html_content)
            parser = Parser(node_id, store)
            processor = Processor(node_id, store)
            
            raw_sale = await scraper.run()
            parsed_sale = await parser.run(raw_sale)
            await processor.run(None)
            
            # print(json.dumps(raw_sale.model_dump(mode="json"), indent=4))
            # print(json.dumps(parsed_sale.model_dump(mode="json"), indent=4))
      
      pass
      
asyncio.run(main())
