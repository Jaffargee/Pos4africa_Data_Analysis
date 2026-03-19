import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

"""
This file is for the in-memory caching layer. It will be used to store the results of the scraping process before they are inserted into the database.
This is to reduce the number of database writes and to allow for faster retrieval of data for analysis. 
And also they are data that needs to be accessed frequently during the scraping process, such as the list of sale IDs that have already been scraped, or the results of the scraping process that are still being processed.
"""

class InMemoryCache:
      def __init__(self):
            self.scraped_sales = set()  # To store IDs of already scraped sales
            self.scrape_results = {}     # To store results of scraping, keyed by sale ID
            self.customers_info = {}       # To store customer information, keyed by customer ID
            self.accounts_info = {}        # To store account information, keyed by account ID

      def add_scraped_sale(self, sale_id: int):
            self.scraped_sales.add(sale_id)

      def is_scraped(self, sale_id: int) -> bool:
            return sale_id in self.scraped_sales

      def add_scrape_result(self, sale_id: int, result: dict):
            self.scrape_results[sale_id] = result

      def get_scrape_result(self, sale_id: int) -> dict | None:
            return self.scrape_results.get(sale_id)

      def get_customer_by_name(self, name: str) -> dict | None:
            return self.customers_info.get(name)

class InMemoryCacheManager:
      def __init__(self, supabase_client):
            self.cache = InMemoryCache()
            self.supabase = supabase_client

      async def load_customers(self):
            """Load all customers from supabase into cache on startup"""
            result = self.supabase.table("customers").select("*").execute()
            for customer in result.data:
                  # key by name for lookup during scraping
                  self.cache.customers_info[customer["name"]] = customer
            logger.info(f"Loaded {len(result.data)} customers into cache")
                  
      async def load_accounts(self):
            """Load all accounts from supabase into cache on startup"""
            result = self.supabase.table("accounts").select("*").execute()
            for account in result.data:
                  # key by account name for lookup during scraping
                  self.cache.accounts_info[account["account_name"]] = account
            logger.info(f"Loaded {len(result.data)} accounts into cache")

      def get_customer_id_by_name(self, name: str) -> int | None:
            customer = self.cache.get_customer_by_name(name)
            return customer["pos_customer_id"] if customer else None

      def add_customer(self, customer: dict):
            """Add newly discovered customer to cache without re-fetching"""
            self.cache.customers_info[customer["name"]] = customer