# # tester.py

# import asyncio

# from pos4africa.worker.components.egress import WorkerEgress
# from pos4africa.worker.node import WorkerNode
# from pos4africa.manager.registry import WorkerRegistry
# from pos4africa.manager.scheduler import Scheduler
# from pos4africa.shared.models.job import ScrapeJob
# from pos4africa.manager.egress.consumer import ManagerEgressConsumer
# from pos4africa.manager.memory.store import MemoryStore
# from pos4africa.infra.redis_client import get_redis
# from pos4africa.config.settings import settings
# from uuid import uuid5, NAMESPACE_DNS
# from hashlib import sha256

# async def worker_coroutine(node, node_id):
#       for i in range(1, 500):
#             await node.submit_job(ScrapeJob(node_id=node_id, pos_sale_id=i))
      
#       tasks = set()
      
#       while True:
#             try:
#                   job = await asyncio.wait_for(node._job_queue.get(), timeout=5.0)
                  
#                   task = asyncio.create_task(node._handle_job(job))
                  
#                   tasks.add(task)
                  
#                   task.add_done_callback(tasks.discard)
                  
#             except asyncio.TimeoutError:
#                   continue
#             except Exception:
#                   print("Exception is raised based on i fucking dont know.")

# async def main():
      
#       nodes: list[WorkerNode] = []
#       registry = WorkerRegistry()
#       scheduler = Scheduler(registry)
#       consumer_egress_manager = ManagerEgressConsumer()
#       redis = await get_redis()      
#       node_id = sha256(str(uuid5(NAMESPACE_DNS, f"node_{1+1:02d}")).encode()).hexdigest()
#       memstore = MemoryStore(node_id, redis)
#       egress = WorkerEgress(node_id, memstore)
#       node = WorkerNode(node_id=node_id)
#       node._memory = memstore
#       node._egress = egress
      
#       await asyncio.gather(consumer_egress_manager.run(), worker_coroutine(node, node_id))
      
#       # nodes.append(node)
      
#       # registry.register(node_id, node)
            
#       # await asyncio.gather(node.start(), scheduler.run(), consumer_egress_manager.run())
      
#       pass

# if __name__ == "__main__":
#       try:
#             asyncio.run(main())
#       except KeyboardInterrupt:
#             print("Keyboard Interupted.")



# tester.py
import asyncio
from hashlib import sha256
from uuid import uuid5, NAMESPACE_DNS

from pos4africa.infra.redis_client import get_redis
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.models.job import ScrapeJob, BatchJob
from pos4africa.worker.components.connector import PosConnector
from pos4africa.worker.components.scraper import Scraper
from pos4africa.worker.components.parser import Parser
from pos4africa.worker.components.processor import Processor
from pos4africa.worker.components.dedup_guard import DedupGuard
from pos4africa.worker.components.egress import WorkerEgress
from pos4africa.worker.components.health_reporter import HealthReporter


# ── Config ────────────────────────────────────────────────────────────────────
TEST_SALE_IDS = [ScrapeJob(pos_sale_id=id) for id in range(1, 510)]   # the IDs you were testing


def make_node_id(n: int) -> str:
      return sha256(str(uuid5(NAMESPACE_DNS, f"node_{n:02d}")).encode()).hexdigest()


# ── Stage 1: test the connector alone ────────────────────────────────────────
async def test_connector(node_id: str, memory: MemoryStore) -> None:
      print("\n=== STAGE 1: Connector ===")
      async with PosConnector(node_id, memory) as connector:
            for sale_id in TEST_SALE_IDS:
                  html = await connector.run(pos_sale_id=sale_id)
                  is_html = "<html" in html.lower() or "<!doctype" in html.lower()
                  print(f"  sale_id={sale_id}  len={len(html)}  is_html={is_html}")
                  if not is_html:
                        print(f"  PREVIEW: {html[:200]}")
      print("Connector OK\n")


# ── Stage 2: test scraper on fetched HTML ─────────────────────────────────────
async def test_scraper(node_id: str, memory: MemoryStore) -> None:
      print("=== STAGE 2: Connector → Scraper ===")
      scraper = Scraper(node_id, memory)
      async with PosConnector(node_id, memory) as connector:
            for sale_id in TEST_SALE_IDS:
                  html = await connector.run(pos_sale_id=sale_id)
                  try:
                        raw_sale = await scraper.run(sale_id=sale_id, html_content=html)
                        print(f"  sale_id={sale_id}  datetime={raw_sale.invoice_datetime}  "
                              f"items={len(raw_sale.items)}  payments={len(raw_sale.payments)}")
                  except Exception as exc:
                        print(f"  sale_id={sale_id}  SCRAPER ERROR: {exc}")
      print("Scraper done\n")


# ── Stage 3: full pipeline, no egress ────────────────────────────────────────
async def test_pipeline(node_id: str, memory: MemoryStore) -> None:
      print("=== STAGE 3: Full pipeline (no egress) ===")
      scraper   = Scraper(node_id, memory)
      parser    = Parser(node_id, memory)
      processor = Processor(node_id, memory)
      dedup     = DedupGuard(node_id, memory)

      async with PosConnector(node_id, memory) as connector:
            for sale_id in TEST_SALE_IDS:
                  print(f"\n  --- sale_id={sale_id} ---")
                  try:
                        # Dedup
                        if await dedup.run(pos_sale_id=sale_id):
                              print(f"  DUPLICATE — skipped")
                              continue

                        # Fetch
                        html = await connector.run(pos_sale_id=sale_id)
                        print(f"  Fetched: {len(html)} chars")

                        # Scrape
                        raw_sale = await scraper.run(sale_id=sale_id, html_content=html)
                        print(f"  Scraped: datetime={raw_sale.invoice_datetime} "
                              f"items={len(raw_sale.items)}")

                        # Parse
                        parsed_sale = await parser.run(raw_sale=raw_sale)
                        print(f"  Parsed:  {parsed_sale}")

                        # Process
                        processed_sale = await processor.run(parsed_sale=parsed_sale)
                        print(f"  Processed: {processed_sale}")

                  except Exception as exc:
                        print(f"  PIPELINE ERROR at sale_id={sale_id}: {exc}")

      print("\nPipeline done\n")


# ── Stage 4: full node with BatchJob (no egress consumer needed) ──────────────
async def test_node_batch(node_id: str, memory: MemoryStore) -> None:
      print("=== STAGE 4: WorkerNode batch (egress publishes to RabbitMQ) ===")
      from pos4africa.worker.node import WorkerNode

      node = WorkerNode(node_id=node_id)
      # Wire dependencies manually — same as node.start() but without
      # blocking on _job_loop forever
      node._memory = memory
      node._egress = WorkerEgress(node_id, memory)
      node._health = HealthReporter(node_id, memory)
      node._running = True

      jobs = [ScrapeJob(node_id=node_id, pos_sale_id=id) for id in range(1, 510)]
      
      for job in jobs:
            node.submit_job(job)
      
      while True:
            job = await asyncio.wait_for(node._job_queue.get(), timeout=5.0)
            
            task = asyncio.create_task(node._handle_job(job))
            
            tasks.add(task)
            
            task.add_done_callback(tasks.discard)
      
      print("Node batch done\n")


# ── Main: run stages in order, stop at first failure ─────────────────────────
async def main() -> None:
      node_id = make_node_id(1)
      redis   = await get_redis()
      memory  = MemoryStore(node_id, redis)
      await memory.initialise()

      print(f"Node ID: {node_id[:16]}...")

      # Run stages one at a time so you know exactly where it breaks
      await test_connector(node_id, memory)
      await test_scraper(node_id, memory)
      await test_pipeline(node_id, memory)

      # Only run stage 4 once stages 1-3 pass cleanly
      # It needs RabbitMQ running — comment out if not ready
      # await test_node_batch(node_id, memory)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted.")