"""
node.py

Single-node Excel ingestion pipeline:
  ExcelScraper -> Parser -> Processor -> BatchWriter
"""

from __future__ import annotations

from pos4africa.config.settings import settings
from pos4africa.manager.egress import syncer
from pos4africa.manager.egress import batch_writer
from pos4africa.manager.egress.batch_writer import BatchWriter
from pos4africa.manager.memory.store import MemoryStore
from pos4africa.shared.utils.logger import get_logger
from pos4africa.worker.components.dedup_guard import DedupGuard
from pos4africa.worker.components.excel_scraper import ExcelScraper
from pos4africa.worker.components.customer_scraper import CustomerScraper
from pos4africa.worker.components.parser import Parser
from pos4africa.worker.components.processor import Processor
from pos4africa.manager.egress.syncer import Syncer
from pos4africa.infra.supabase_client import spb_client
from pos4africa.shared.models.sale import ProcessedSale

from typing import Any

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

            syncer = Syncer()
            dedup_guard = DedupGuard(self._node_id, self._memory)
            scraper = ExcelScraper(self._node_id, self._memory)
            customerScraper = CustomerScraper(self._node_id, self._memory)
            parser = Parser(self._node_id, self._memory)
            processor = Processor(self._node_id, self._memory)

            raw_sales = await scraper.run(
                  excel_path=settings.excel_source_path,
                  sheet_name=settings.excel_sheet_name,
            )

            customers = await customerScraper.run(excel_path=settings.customer_excel_path, sheet_name=settings.customer_sheet_name)
            customers = syncer.sync(customers)

            inserted_customers = await self._writer.write_customers(syncer.normalize_customer_data(customers))
            syncer.save_json_object(syncer.customers_local_db_file_path, syncer.get_customers())


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

            # processed_sales = await self.reconcile_and_filter(processed_sales)
            inserted = await self._writer.write(processed_sales)

            summary = {
                  "loaded": len(raw_sales),
                  "inserted": inserted,
                  "inserted_customers": inserted_customers,
                  "duplicates": duplicates,
                  "failed": failed,
            }
            self.log.info("worker_node.completed", **summary)
            return summary

      async def reconcile_and_filter(self, processed_sales: list[dict]) -> list[dict[str, Any]]:
            if not processed_sales:
                  return []

            # Step A: Collect incoming POS sale IDs & compute local hashes
            local_sales_by_id = {sale["pos_sale_id"]: sale for sale in processed_sales}
            sale_ids = list(local_sales_by_id.keys())

            # Step B: Fetch existing remote hashes from Supabase in batches
            remote_records = {}
            chunk_size = 1000
            
            for i in range(0, len(sale_ids), chunk_size):
                  chunk_ids = sale_ids[i:i + chunk_size]
                  res = (
                        spb_client.table("sales")
                        .select("pos_sale_id, hash")
                        .in_("pos_sale_id", chunk_ids)
                        .execute()
                  )
                  if res.data:
                        for row in res.data:
                              remote_records[row["pos_sale_id"]] = row["hash"]

            # Step C: Correlate and isolate discrepancies
            records_to_upsert = []
            
            for pos_sale_id, sale in local_sales_by_id.items():
                  remote_hash = remote_records.get(pos_sale_id)

                  # Discrepancy / New Record Found
                  if remote_hash != sale["hash"]:
                        records_to_upsert.append(sale)

            self.log.info(
                  "reconcile.complete",
                  total_incoming=len(processed_sales),
                  discrepancies_or_new=len(records_to_upsert),
                  skipped_unmodified=len(processed_sales) - len(records_to_upsert)
            )

            return records_to_upsert