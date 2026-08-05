from pos4africa.worker.pipeline.base import BaseComponent, PipelineContext
from pos4africa.worker.components.extractors.catalog_extractor import CatalogExtractor
from pos4africa.manager.egress.batch_writer import BatchWriter
from pos4africa.config.settings import settings

from typing import Any

class CatalogSyncer(BaseComponent):
      async def run(self, ctx: PipelineContext) -> None:

            writer = BatchWriter()
            catalog_extractor = CatalogExtractor(ctx.node_id, ctx.memory)

            items = await catalog_extractor.run(excel_path=settings.catalog_excel_path, sheet_name=settings.customer_sheet_name)
            items: list[dict[str, Any]] = [item.model_dump(mode="dict") for item in items]
            inserted_catalog = await writer.write_catalogs(items)

            ctx.metrics["inserted_catalog"] = inserted_catalog