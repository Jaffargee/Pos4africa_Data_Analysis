from pos4africa.worker.components.syncer import Syncer
from pos4africa.manager.host import HostManager
from pos4africa.worker.components.extractors.catalog_extractor import CatalogExtractor
from pos4africa.manager.egress.batch_writer import BatchWriter
from pos4africa.config.settings import settings
from .auto_sync import DownloadManager, TODAY_REPORT_URL
from pathlib import Path
import asyncio
import json


async def main():
      excel_path = Path(settings.catalog_excel_path).resolve()
      node_id = "test_node"
      memory_store = None
      writer = BatchWriter()
      extractor = CatalogExtractor(node_id=node_id, memory=memory_store)
      items = await extractor.run(excel_path=excel_path, sheet_name=settings.excel_sheet_name)

      print(excel_path, excel_path.is_absolute())
      items = [item.model_dump(mode="json", exclude_none=True) for item in items]
      print(json.dumps(items, indent=4))

      catalog_inserted = await writer.write_catalogs(items)
      print(catalog_inserted)

      # manager = HostManager()
      # await manager.run()



asyncio.run(main())