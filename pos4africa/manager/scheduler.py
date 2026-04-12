"""
scheduler.py — Scheduler

Decides what each worker node scrapes and when.

Strategy:
  - On startup, load all terminal refs from Supabase
  - Partition terminals across nodes (each node owns a slice)
  - Every scrape_interval seconds, generate a new batch of ScrapeJobs
    for the current date range and push them to the relevant nodes
  - Handles node failures by redistributing unfinished jobs

Job assignment: terminal_ref → node via consistent hashing so the same
terminal always goes to the same node (keeps dedup cache warm).
"""

from __future__ import annotations

import asyncio
import hashlib
from datetime import date, timedelta

from pos4africa.config.settings import settings
from pos4africa.manager.registry import WorkerRegistry
from pos4africa.shared.models.job import ScrapeJob
from pos4africa.shared.utils.logger import get_logger
import math

log = get_logger(__name__)

_SCRAPE_INTERVAL = 300  # seconds between full scrape cycles (5 minutes)
_OLD_DATA_MAX_SALES_TRANSACTION = 16_028
_CHUNK_SIZE = 1000  # Process 1000 jobs at a time
_CHUNK_DELAY = 2  # Wait 2 seconds between chunks

class Scheduler:
      def __init__(self, registry: WorkerRegistry) -> None:
            self._registry = registry

      async def run(self) -> None:
            while True:
                  try:
                        await self._dispatch_cycle()
                  except Exception as exc:
                        log.error("scheduler.cycle_error", error=str(exc))
                        await asyncio.sleep(_SCRAPE_INTERVAL)

      async def _load_pos_sale_ids(self) -> None:
            return [ScrapeJob(pos_sale_id=id) for id in range(1, _OLD_DATA_MAX_SALES_TRANSACTION + 1)]
             
      async def _dispatch_cycle(self) -> None:
            dispatched = 0
            total_jobs = _OLD_DATA_MAX_SALES_TRANSACTION
            
            for chunk_start in range(1, _OLD_DATA_MAX_SALES_TRANSACTION + 1, _CHUNK_SIZE):
                  chunk_end = min(chunk_start + _CHUNK_SIZE, _OLD_DATA_MAX_SALES_TRANSACTION + 1)
                  chunk_size = chunk_end - chunk_start
                  
                  log.info(
                        "scheduler.processing_chunk",
                        start=chunk_start,
                        end=chunk_end - 1,
                        size=chunk_size
                  )
                  
                  chunk_jobs = [ScrapeJob(pos_sale_id=id) for id in range(chunk_start, chunk_end)]
            
                  for job in chunk_jobs:
                        node = self._assign_node(job.pos_sale_id)
                        
                        if node is None:
                              log.warning("scheduler.no_node_available")
                              continue

                        job.node_id = node.node_id
                        
                        await node.submit_job(job)
                        dispatched += 1

                              
                  log.info(
                        "scheduler.chunk_dispatched",
                        chunk_start=chunk_start,
                        chunk_size=chunk_size,
                        total_dispatched=dispatched
                  )
                  
                              # Give workers time to process the chunk
                  if chunk_end <= total_jobs:
                        await asyncio.sleep(_CHUNK_DELAY)
        
            log.info(
                  "scheduler.cycle_completed",
                  total_dispatched=dispatched,
                  total_jobs=total_jobs
            )

      def _assign_node(self, pos_sale_id: int):
            """
            Consistent hashing: same pos_sale_id always maps to the same node.
            This keeps the dedup Redis SET warm for that node.
            """
            node_ids = self._registry.node_ids
            if not node_ids:
                  return None
            
            digest = int(hashlib.md5(str(pos_sale_id).encode(), usedforsecurity=False).hexdigest(), 16)
            node_id = node_ids[digest % len(node_ids)]
            return self._registry.get(node_id)