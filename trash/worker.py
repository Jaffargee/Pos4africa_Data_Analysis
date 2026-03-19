import asyncio
import logging
from dataclasses import dataclass
import httpx
import os
from dotenv import load_dotenv
from supabase_client import supabase
from inmemcache import InMemoryCacheManager
from data_extractor import DataExtractor
import json

load_dotenv()  # Load environment variables from .env file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POS_BASE = os.getenv("POS_BASE")
POS_USER = os.getenv("POS_USERNAME")
POS_PASS = os.getenv("POS_PASSWORD")
DEFAULT_TIMEOUT = 30  # seconds

class Worker:
      def __init__(self, name: str, start_id: int, end_id: int):
            self.name = name
            self.start_id = start_id
            self.end_id = end_id
            self.success = 0
            self.failed = []
            self.client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT)
            self.logged_in = False
            # Initialize in-memory cache manager
            self.cache_manager = InMemoryCacheManager(supabase)
            
      def parse_receipt(self, html: str, sale_id: int) -> dict:
            # This function will use BeautifulSoup to parse the receipt HTML and extract the relevant data into a structured dict format.
            # The implementation will depend on the structure of the receipt HTML, but it will likely involve selecting elements by their classes or IDs and extracting text content.
            data_extractor = DataExtractor(html)
            return data_extractor.get_sale_items()
            
      async def initialize(self):
            await self.login()
            await self.cache_manager.load_accounts()  # Load accounts into cache on startup
            await self.cache_manager.load_customers()  # Load customers into cache on startup
            logger.info(f"[{self.name}] Initialized and ready")

      async def login(self):
            logger.info(f"{self.name} logging in.")
            try:
                  await self.client.post(f"{POS_BASE}/index.php/login", data={"username": POS_USER, "password": POS_PASS})
                  self.logged_in = True
                  logger.info(f"{self.name} logged in successfully.")
            except Exception as e:
                  logger.error(f"{self.name} failed to log in: {e} -> [CHECK URL, CREDENTIALS, AND CONNECTION]")
                  self.logged_in = False
            
      async def scrape_one(self, sale_id: int) -> dict | None:
            try:
                  resp = await self.client.get(f"{POS_BASE}/index.php/sales/receipt/{sale_id}", timeout=DEFAULT_TIMEOUT)
                  if resp.status_code == 404:
                        return None
                  return self.parse_receipt(resp.text, sale_id)
            except Exception as e:
                  logger.warning(f"{self.name} Sale {sale_id} failed: {e}")
                  return None
      
      async def work(self):
            await self.initialize()  # ← replaces manual login + cache load
            logger.info(f"{self.name} is working.")
            sales_data = await self.scrape_one(15680)  # Example sale ID for testing
            if sales_data:
                  logger.info(f"{self.name} scraped data for sale ID 15680: {sales_data}")
                  for sale in sales_data:
                        print(sale)
            else:
                  logger.info(f"{self.name} no data found for sale ID 15680.")
                  
                  
                  
async def main():
    worker = Worker("Worker-1", 15600, 15699)
    await worker.work()

asyncio.run(main())
            
            
            

"""
{
      "invoice_total": 642000,
      "invoice_policy": "",
      "invoice_payments": [
            {
                  "account": "MONIEPOINT",
                  "amount": 642000,
                  "transaction_medium": "Bank Transfer",
                  "account_id": 2
            }
      ],
      "total_items_count": 7,
      "invoice_to": " MUSTAPHA SHOP",
      "invoice_datetime": "03/13/2026 05:50 pm",
      "sales_id": 15635,
      "items_count": 5,
      "items": [
            {
                  "sale_id": 15635,
                  "item_id": 460,
                  "name": "Discount",
                  "qty": -1,
                  "unit_price": 100,
                  "total": -100
            },
            {
                  "sale_id": 15635,
                  "item_id": 578,
                  "name": "New Medium Glitter",
                  "qty": 1,
                  "unit_price": 19500,
                  "total": 19500
            },
            {
                  "sale_id": 15635,
                  "item_id": 421,
                  "name": "HOLLAND MEDIUM",
                  "qty": 2,
                  "unit_price": 11800,
                  "total": 23600
            },
            {
                  "sale_id": 15635,
                  "item_id": 332,
                  "name": "ABS SUPER Embellished",
                  "qty": 1,
                  "unit_price": 29000,
                  "total": 29000
            },
            {
                  "sale_id": 15635,
                  "item_id": 316,
                  "name": "NEW SUPER HOLLAND",
                  "qty": 3,
                  "unit_price": 190000,
                  "total": 570000
            }
      ]
}
"""