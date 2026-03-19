from pos4africa.worker.components.scraper import Scraper
from pos4africa.worker.components.parser import Parser
from pos4africa.shared.models.sale import SaleItem, RawPayment
from datetime import datetime
import json

receipt = open('./test/fixtures/receipt.html', 'r', encoding='utf-8')

scraper = Scraper(15743, receipt)
parser = Parser()
raw_sale = scraper.run()
parsed_sale = parser.run(raw_sale)

print(json.dumps(raw_sale.model_dump(mode="json"), indent=4))
print(json.dumps(parsed_sale.model_dump(mode="json"), indent=4))

