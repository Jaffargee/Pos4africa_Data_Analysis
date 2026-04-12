
from __future__ import annotations

from pos4africa.config.settings import settings
from pos4africa.shared.utils.logger import get_logger

from typing import Callable, Awaitable

import asyncio
import math

log = get_logger(__name__)

class BatchProcessor:
      
      def __init__(self, data: list, batch_size: int = settings.worker_count) -> None:
            self._data = data
            self._batch_size = batch_size
            self._data_len = len(data)
            self._batches = []
                                    
      def get_batch_size(self) -> int:
            return self._batch_size
      
      def get_batch_len(self) -> int:
            return math.ceil(self._data_len / self._batch_size)
      
      def get_total_batches(self) -> int:
            return math.ceil(self._data_len / self.get_batch_len())

      def get_batches(self) -> list:
            return [
                  self._data[i : i + self.get_batch_len()] 
                  for i in range(0, len(self._data), self._batch_size)
            ]
            
      async def run_in_batches(self, callback: Callable[[list], Awaitable[None]]) -> None:
            """
            Run callback on batches concurrently with semaphore control.
            
            Args:
                  callback: Async function that processes a single batch
                  max_concurrent: Maximum number of concurrent batch executions
            """
            semaphore = asyncio.Semaphore(settings.worker_count)
            
            
            async def process_batch(batch_idx: int, batch: list) -> None:
                  async with semaphore:
                        try:
                              log.debug("batch.processing", batch_idx=batch_idx, size=len(batch))
                              await callback(batch)
                              log.debug("batch.completed", batch_idx=batch_idx)
                        except Exception as exc:
                              log.error("batch.failed", batch_idx=batch_idx, error=str(exc))
                              raise
                        
            tasks = [
                  asyncio.create_task(process_batch(batch_idx=batch_idx, batch=batch))
                  for batch_idx, batch in enumerate(self.get_batches(), 1)
            ]
            
            results  = await asyncio.gather(*tasks, return_exceptions=True)
            
            successful = sum(1 for _ in results if not isinstance(_, Exception))
            failed = len(results) - successful
            
            log.info(
                  "batch_processing.completed",
                  total_batches=len(self.get_batches()),
                  successful=successful,
                  failed=failed
            )