from pos4africa.worker.components.scraper import Scraper
from pos4africa.worker.components.parser import Parser
from pos4africa.worker.components import Sync
from pos4africa.config import settings
from pos4africa.shared.models.sale import SaleItem, RawPayment
from datetime import datetime
import json

print(settings.supabase_key.get_secret_value(), settings.supabase_url)

# receipt = open('./test/fixtures/receipt.html', 'r', encoding='utf-8')

# scraper = Scraper(15743, receipt)
# parser = Parser()
# raw_sale = scraper.run()
# parsed_sale = parser.run(raw_sale)

# print(json.dumps(raw_sale.model_dump(mode="json"), indent=4))
# print(json.dumps(parsed_sale.model_dump(mode="json"), indent=4))

# for ctm in Sync.fetch_customers():
#       print('========================================================\n')
#       print(ctm)
# print('------------------------------------------------')
# for acct in Sync.fetch_accounts():
#       print('========================================================\n')
#       print(acct)

