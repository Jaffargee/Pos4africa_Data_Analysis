from pos4africa.worker.components.scraper import Scraper
from pos4africa.shared.models.sale import SaleItem, RawPayment
from datetime import datetime
import json

receipt = open('./test/fixtures/receipt.html', 'r', encoding='utf-8')

scraper = Scraper(15743, receipt)

