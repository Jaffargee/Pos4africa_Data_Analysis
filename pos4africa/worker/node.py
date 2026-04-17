"""
node.py

Single-node Excel ingestion pipeline:
  ExcelScraper -> Parser -> Processor -> BatchWriter
"""

from __future__ import annotations

from pos4africa.config.settings import settings
from pos4africa.manager.egress.batch_writer import BatchWriter
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.utils.logger import get_logger
from pos4africa.worker.components.dedup_guard import DedupGuard
from pos4africa.worker.components.excel_scraper import ExcelScraper
from pos4africa.worker.components.parser import Parser
from pos4africa.worker.components.processor import Processor


class WorkerNode:
      def __init__(self, node_id: str) -> None:
            self._node_id = node_id
            self.log = get_logger(__name__).bind(node_id=node_id)
            self._running = False
            self._memory: MemoryStore | None = None
            self._writer = BatchWriter()

      @property
      def node_id(self) -> str:
            return self._node_id

      async def start(self) -> None:
            self._memory = MemoryStore(self._node_id)
            await self._memory.initialise()
            self._running = True
            self.log.info("worker_node.started", excel_source_path=settings.excel_source_path)

      async def stop(self) -> None:
            self._running = False
            if self._memory:
                  await self._memory.close()
            self.log.info("worker_node.stopped")

      async def run_once(self) -> dict[str, int]:
            if not self._running or self._memory is None:
                  raise RuntimeError("WorkerNode must be started before run_once().")

            dedup_guard = DedupGuard(self._node_id, self._memory)
            scraper = ExcelScraper(self._node_id, self._memory)
            parser = Parser(self._node_id, self._memory)
            processor = Processor(self._node_id, self._memory)

            raw_sales = await scraper.run(
                  excel_path=settings.excel_source_path,
                  sheet_name=settings.excel_sheet_name,
            )

            processed_sales: list[dict] = []
            duplicates = 0
            failed = 0

            for raw_sale in raw_sales:
                  sale_id = int(raw_sale.pos_sale_id) if raw_sale.pos_sale_id else None
                  if sale_id is None:
                        failed += 1
                        continue

                  if await dedup_guard.run(pos_sale_id=sale_id):
                        duplicates += 1
                        continue

                  try:
                        parsed_sale = await parser.run(raw_sale=raw_sale)
                        processed_sale = await processor.run(parsed_sale=parsed_sale)

                        if processed_sale is None:
                              failed += 1
                              continue

                        processed_sales.append(processed_sale.to_db_dict())
                  except Exception as exc:
                        failed += 1
                        self.log.error(
                              "worker_node.sale_failed",
                              pos_sale_id=sale_id,
                              error=str(exc),
                        )

            inserted = await self._writer.write(processed_sales)

            summary = {
                  "loaded": len(raw_sales),
                  "inserted": inserted,
                  "duplicates": duplicates,
                  "failed": failed,
            }
            self.log.info("worker_node.completed", **summary)
            return summary
