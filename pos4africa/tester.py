from pos4africa.manager.egress.syncer import Syncer
from pos4africa.manager.host import HostManager
from pos4africa.worker.components.extractors.sale_extractor import SaleExtractor
from pos4africa.config.settings import settings
from .auto_sync import DownloadManager, TODAY_REPORT_URL
from pathlib import Path
import asyncio
import json


async def main():
      # excel_path = Path(settings.excel_source_path)
      # node_id = "test_node"
      # memory_store = None
      # scraper = ExcelScraper(node_id=node_id, memory=memory_store)
      # raw_sales = await scraper.run(excel_path=excel_path, sheet_name=settings.excel_sheet_name)
      # print("Scraped Sales:", json.dumps([sale.dict() for sale in raw_sales], indent=4))


      manager = HostManager()
      await manager.run()
      # download_manager = DownloadManager()
      # # Download the Excel file
      # await download_manager.download_excel(
      #       url=TODAY_REPORT_URL,
      #       destination=Path(settings.excel_source_path)
      # )


asyncio.run(main())