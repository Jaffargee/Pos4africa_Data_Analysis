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
from pos4africa.shared.models.job import ScrapeJob, JobStatus
from pos4africa.shared.models.sale import ProcessedSale
from pos4africa.shared.utils.logger import get_logger

import asyncio

_CONCURRENT_PER_NODE = 5
_BUFFER_FLUSH_INTERVAL = 30


class JobException(Exception):
      """ Raise when job failed to processed """
      pass

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
            self._egress: WorkerEgress | None = None
            
            self._background_tasks: Set[asyncio.Task] = set()

            
      @property
      def node_id(self) -> str:
            return self._node_id
            
      async def start(self) -> None:
            redis: RedisClient = await get_redis()
            self._memory = MemoryStore(self._node_id, redis=redis)
            self._egress = WorkerEgress(self._node_id, self._memory)
            self._health: HealthReporter = HealthReporter(self._node_id, self._memory)
            await self._health.start()
            await self._memory.initialise()
            self._running = True
            self.log.info("worker_node.started")
            
            # job_loop_task = asyncio.create_task(self._job_loop(), name=f"{self._node_id}_job_loop")
            # buffer_task = asyncio.create_task(self._buffer_flush_loop(), name=f"{self._node_id}_buffer")
            
            # # Keep references so they don't get garbage collected
            # self._background_tasks.add(job_loop_task)
            # self._background_tasks.add(buffer_task)
            
            # # Clean up references when tasks complete
            # job_loop_task.add_done_callback(self._background_tasks.discard)
            # buffer_task.add_done_callback(self._background_tasks.discard)
            
            # await asyncio.gather(self._job_loop(), self._buffer_flush_loop())
            
            await self._job_loop()
            
            self.log.info("worker_node.background_tasks_started")
            
      async def stop(self) -> None:
            self._running = False
            if self._health:
                  await self._health.stop()
            if self._memory:
                  await self._memory.close()
            self.log.info("worker_node.stopped")
            
      async def _buffer_flush_loop(self) -> None:
            while self._running:
                  await asyncio.sleep(_BUFFER_FLUSH_INTERVAL)
                  if self._egress:
                        await self._egress.flush_retry_buffer()
                        
      async def submit_job(self, job: ScrapeJob) -> None:
            await self._job_queue.put(job)
                        
      # Job Coroutine for running multiple task concurently
      async def _job_loop(self) -> None:
            
            tasks = set()
            
            while True:
                  try:
                        job = await asyncio.wait_for(self._job_queue.get(), timeout=5.0)
                        
                        task = asyncio.create_task(self._handle_job(job))
                        
                        tasks.add(task)
                        
                        task.add_done_callback(tasks.discard)
                        
                  except asyncio.TimeoutError:
                        continue
                  except Exception as exc:
                        self.log.error("worker_node.job_loop_error", error=str(exc))
                        
      async def _handle_job(self, job: ScrapeJob) -> None:
            async with PosConnector(self._node_id, self._memory) as connector:
                  await self._run_job(connector=connector, job=job)
                  self._job_queue.task_done()
                 
      async def _run_job(self, connector: PosConnector, job: ScrapeJob) -> None | bool:
            """_summary_

            Args:
                  connector (PosConnector): A Single POS Connection for a single Worker Node across all grouped task coroutine.
                  job: ScrapeJob
            """
            """Semaphore-gated pipeline for a single sale."""
            async with self._semaphore:
                  try:
                        job.mark_started()
                        
                        self.log.info("worker_node.job_started", node_id=str(self._node_id), attempt=job.attempt)
                        
                        processed = await self._pipeline(connector=connector, job=job)
                        
                        if processed is False:
                              job.mark_done()
                              self.log.info("worker_node.job_skipped", pos_sale_id=job.pos_sale_id)
                              return None
                        
                        if processed is None:
                              raise JobException("Processing failed")
                        
                        job.mark_done()
                        self.log.info("worker_node.batch_complete", pos_sale_id=job.pos_sale_id)
                        
                        await self._egress.run(processed.model_dump(mode="json"))
                        
                        return True
                        
                  except JobException as job_exc:
                        job.mark_failed(str(job_exc))
                        self.log.error("worker_node.pipeline_error", pos_sale_id=job.pos_sale_id, error=str(job_exc), attempt=job.attempt)
                        
                        # Check if we should retry
                        if job.status == JobStatus.RETRYING:
                              self.log.info(
                                    "worker_node.job_retrying",
                                    pos_sale_id=job.pos_sale_id,
                                    attempt=job.attempt,
                                    max_retries=job.max_retries
                              )
                        
                        return None

      async def _pipeline(self, connector: PosConnector, job: ScrapeJob) -> ProcessedSale | bool:
            
            try:
                  dedup_guard = DedupGuard(self._node_id, self._memory)
                  scraper     = Scraper(self._node_id, self._memory)
                  parser      = Parser(self._node_id, self._memory)
                  processor   = Processor(self._node_id, self._memory)
                  egress      = WorkerEgress(self._node_id, self._memory)
                  
                  # self._health.register(scraper, dedup_guard, parser, processor, egress)
                              
                  if await dedup_guard.run(pos_sale_id=job.pos_sale_id):
                        self.log.info("worker_node.duplicate_skipped", pos_sale_id=job.pos_sale_id)
                        return False
                  
                  html = await connector.run(pos_sale_id=job.pos_sale_id)
                  
                  raw_sale       = await scraper.run(sale_id=job.pos_sale_id, html_content=html)
                  parsed_sale    = await parser.run(raw_sale=raw_sale)
                  processed_sale = await processor.run(parsed_sale=parsed_sale)
                  
                  return processed_sale
      
            except Exception as exc:
                  self.log.error("Pipeline Processing Error", pos_sale_id=job.pos_sale_id, error=str(exc))
                  return False