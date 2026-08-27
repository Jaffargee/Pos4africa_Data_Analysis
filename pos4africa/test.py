import asyncio

from pos4africa.worker.components.extractors.sale_extractor import SaleExtractor
from pos4africa.config.settings import settings
from pos4africa.worker.components.parser import Parser
from pos4africa.worker.components.processor import Processor
from pos4africa.manager.memory.store import MemoryStore

async def main() -> None:

      memstore = MemoryStore()
      await memstore.initialise()
      extrct = SaleExtractor('node-123', memstore)
      parser = Parser('node-123', memstore)
      processor = Processor('node-123', memstore)
      await processor.initialize()

      raw_sales = await extrct.run(excel_path=settings.excel_source_path)
      failed = 0
      processed = 0
      processed_sales: list = []
      # raw_sales = raw_sales[1000: 1005]

      for raw_sale in raw_sales:
            sale_id = int(raw_sale.pos_sale_id) if raw_sale.pos_sale_id else None

            if sale_id is None:
                  failed += 1
                  continue

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
            processed += 1


      print("Failed Sales: ", failed, "Processed: ", processed)

asyncio.run(main())