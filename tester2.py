import asyncio
from pprint import pprint

from pos4africa.worker.components.excel_scraper import ExcelScraper
from pos4africa.worker.components.parser import Parser
from pos4africa.worker.components.processor import Processor


async def main() -> None:
      excel_path = "./Excels/DSR.xlsx"

      excel_scraper = ExcelScraper(node_id="excel-test", memory=None)  # type: ignore[arg-type]
      parser = Parser(node_id="excel-test", memory=None)  # type: ignore[arg-type]
      processor = Processor(node_id="excel-test", memory=None)  # type: ignore[arg-type]

      raw_sales = await excel_scraper.run(excel_path=excel_path)

      print(f"Loaded {len(raw_sales)} grouped sales from {excel_path}")

      for raw_sale in raw_sales[:3]:
            parsed_sale = await parser.run(raw_sale)
            processed_sale = await processor.run(parsed_sale)

            print("\nRAW SALE")
            pprint(raw_sale.model_dump())

            print("\nPARSED SALE")
            pprint(parsed_sale.model_dump(mode="json"))
            
            print("\nPROCESSED SALE")
            pprint(processed_sale)


if __name__ == "__main__":
      asyncio.run(main())
