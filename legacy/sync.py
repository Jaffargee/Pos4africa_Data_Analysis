from pos4africa.infra.supabase_client import spb_client
from pos4africa.config import settings
from pos4africa.shared.models.customer import Customer
from pos4africa.shared.models.account import Account
import socket, time

class Sync:
      
      _MAX_RETRIES = 3
      _RETRY_DELAY = 1.5 # Seconds 1.5s (basic backoff)
      
      @staticmethod
      async def _select_all(table: str = settings.supabase_table_customers, columns: str = "id, pos_customer_id, first_name") -> list[dict[str, any]]:
            if not table:
                  raise ValueError("Table must be provided.")
            
            last_exc: Exception | None = None

            for attemps in range(Sync._MAX_RETRIES):
                  try:
                        response = spb_client.table(table).select(columns).execute()
                        
                        if not response or response.data is None:
                              return []
                        
                        return response.data
                  
                  except (TimeoutError, socket.gaierror, ConnectionError) as e:
                        last_exc = e
                        time.sleep(Sync._RETRY_DELAY * (attemps + 1))
                  
                  except Exception as e:
                        raise RuntimeError(f"Supabase query failed for table '{table}': {e}") from e
            
            raise ConnectionError(f"Failed to fetch '{table}' after retries") from last_exc
      
      @staticmethod
      async def fetch_customers() -> list[Customer] | None:
            if not settings.supabase_table_customers:
                  raise ValueError('supabase_table_customers is not configured.')

            customers = await Sync._select_all()
            
            return [Customer(**row) for row in customers if row.get("first_name")]
      
      @staticmethod
      async def fetch_accounts() -> list[Account]:
            if not settings.supabase_table_accounts:
                  raise ValueError('supabase_table_accounts is not configured.')

            accounts = await Sync._select_all(table=settings.supabase_table_accounts, columns="id, bank_name") 
            
            return [Account(**acct) for acct in accounts if acct.get("id")]
