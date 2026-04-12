"""
node.py — WorkerNode

The WorkerNode is the self-contained unit that:
  1. Receives a ScrapeJob from the Manager
  2. Runs the full pipeline: Connector → Scraper → Parser → Processor → Egress
  3. Reports health to the Manager
  4. Handles per-job errors without crashing the node

Pipeline flow per job:
  ┌───────────┐    pages     ┌─────────┐   row ids  ┌────────┐
  │ Connector │ ──────────▶  │ Scraper │ ─────────▶ │ Parser │
  └───────────┘              └─────────┘             └────┬───┘
                                                          │ parsed ids
                                                     ┌────▼──────┐
                                  ┌──── dedup ──────▶│ Processor │
                                  │                  └────┬──────┘
                                  │                       │ processed ids
                                  │                  ┌────▼────┐
                                  └──────────────────│ Egress  │
                                                     └─────────┘
"""

from __future__ import annotations

# IMPORTS NODE COMPONENTS
from pos4africa.worker.components.connector import PosConnector
from pos4africa.worker.components.scraper import Scraper
from pos4africa.worker.components.parser import Parser
from pos4africa.worker.components.processor import Processor
from pos4africa.worker.components.egress import WorkerEgress
from pos4africa.worker.components.dedup_guard import DedupGuard
from pos4africa.worker.components.health_reporter import HealthReporter

# IMPORTS NODE DEPENDENCIES
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.infra.redis_client import get_redis, RedisClient
from pos4africa.shared.models.job import ScrapeJob

import asyncio

_CONCURRENT_PER_NODE = 5
_BUFFER_FLUSH_INTERVAL = 30

class WorkerNode:
      def __init__(self, node_id: str) -> None:
            self._node_id: str = node_id
            self._job_queue: asyncio.Queue[ScrapeJob] = asyncio.Queue()
            self._semaphore: asyncio.Semaphore = asyncio.Semaphore(_CONCURRENT_PER_NODE)
            self.log = get_logger(__name__).bind(node_id=node_id)
            self._running: bool = False 
            
            # Dependencies
            self._memory: MemoryStore | None = None
            self._health: HealthReporter | None = None
            
      async def start(self) -> None:
            redis: RedisClient = await get_redis()
            self._memory = MemoryStore(self._node_id, redis=redis)
            self._health: HealthReporter = HealthReporter(self._node_id, self._memory)
            self._running = True
            self.log.info("worker_node.started")
            self._health.start()
                        
            asyncio.gather(self._job_loop(), self._buffer_flush_loop())

      async def stop(self) -> None:
            self._running = False
            if self._health:
                  await self._health.stop()
            self.log.info("worker_node.stopped")
            
      async def _buffer_flush_loop(self) -> None:
            while self._running:
                  await asyncio.sleep(_BUFFER_FLUSH_INTERVAL)
                  if self._egress:
                        await self._egress.flush_retry_buffer()
                        
      async def _job_loop(self) -> None:
            while self._running:
                  try:
                        job = asyncio.wait_for(self._job_queue.get(), timeout=5.0)
                        asyncio.create_task(self._run_job(job))
                        self._job_queue.task_done()
                  except asyncio.TimeoutError:
                        continue
                  except Exception as exc:
                        self.log.error("worker_node.job_loop_error", error=str(exc))
            
      async def _run_job(self, job: ScrapeJob) -> None:
            job.mark_started()
            self.log.info("worker_node.job_started", job_id=str(job.job_id), batch=len(job.pos_sale_ids))

            task = [self._job_coroutine(pos_sale_id=pos_sale_id) for pos_sale_id in job.pos_sale_ids]
            results = asyncio.gather(*task, return_exceptions=True)
            
            records = sum(1 for r in results if r is True)
            errors = sum(1 for r in results if isinstance(r, Exception) or r is True)
            
            if errors:
                  self.log.warning("worker_node.batch_errors", errors=errors)

            job.mark_done(records)
            self.log.info("worker_node.job_done", job_id=str(job.job_id), records=records)

      # Job Coroutine for running multiple task concurently
      async def _job_coroutine(self, pos_sale_id: int) -> None:
            
            async with self._semaphore:
                  try:
                        return self._pipeline(pos_sale_id=pos_sale_id)
                  except Exception as exc:
                        self.log.error("worker_node.sale_failed", pos_sale_id=pos_sale_id, error=str(exc))
                        return False
                  
      async def _pipeline(self, pos_sale_id: int) -> bool:
                        
            dedup_guard = DedupGuard(self._node_id, self._memory)
            scraper     = Scraper(self._node_id, self._memory)
            parser      = Parser(self._node_id, self._memory)
            processor   = Processor(self._node_id, self._memory)
            egress      = WorkerEgress(self._node_id, self._memory)
            
            self._health.register(scraper, dedup_guard, parser, processor, egress)
                        
            if await dedup_guard.run(pos_sale_id=pos_sale_id):
                  self.log.info("worker_node.duplicate_skipped", pos_sale_id=pos_sale_id)
                  return False
            
            async with PosConnector(self._node_id, self._memory) as connector:
                  html = await connector.run(pos_sale_id=pos_sale_id)
            
            raw_sale       = await scraper.run(sale_id=pos_sale_id, html_content=html)
            parsed_sale    = await parser.run(raw_sale=raw_sale)
            processed_sale = await processor.run(parsed_sale=parsed_sale)
            
            return await egress.run(processed_sale=processed_sale)
            
            
            
