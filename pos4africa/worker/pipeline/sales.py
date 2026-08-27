from pos4africa.worker.pipeline.base import BaseComponent, PipelineContext
from pos4africa.worker.components.dedup_guard import DedupGuard
from pos4africa.worker.components.extractors.sale_extractor import SaleExtractor
from pos4africa.worker.components.parser import Parser
from pos4africa.worker.components.processor import Processor
from pos4africa.config.settings import settings
from pos4africa.infra.supabase_client import spb_client
from pos4africa.manager.egress.batch_writer import BatchWriter
from pos4africa.shared.utils.logger import get_logger

from typing import Any

class SalesSyncer(BaseComponent):
      def __init__(self):
            self.log = get_logger(__name__)

      async def run(self, ctx: PipelineContext) -> None:
            writer = BatchWriter()
            dedup_guard = DedupGuard(ctx.node_id, ctx.memory)
            sale_extractor = SaleExtractor(ctx.node_id, ctx.memory)
            parser = Parser(ctx.node_id, ctx.memory)
            processor = Processor(ctx.node_id, ctx.memory)
            await processor.initialize()

            self.log.bind(node_id=ctx.node_id)

            raw_sales = await sale_extractor.run(
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

                        _83exists = [_83 for _83 in processed_sale.items if _83.pos_item_id == 83]
                        if _83exists:
                              for i in _83exists:
                                    i.pos_item_id = 160

                        processed_sales.append(processed_sale.to_db_dict())
                        
                  except Exception as exc:
                        failed += 1
                        self.log.error(
                              "worker_node.sale_failed",
                              pos_sale_id=sale_id,
                              error=str(exc),
                        )

            sales_to_process = await self.reconcile_and_filter(processed_sales)
            # inserted = await writer.write(sales_to_process)
            inserted = await writer.write(processed_sales)

            ctx.metrics["inserted"] = inserted


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