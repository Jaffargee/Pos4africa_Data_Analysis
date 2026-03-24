from pos4africa.infra.supabase_client import spb_client
from pos4africa.config import settings
from pos4africa.shared.models.customer import Customer
from pos4africa.shared.models.account import Account
import socket, time

class Sync:
      
      _MAX_RETRIES = 3
      _RETRY_DELAY = 1.5 # Seconds 1.5s (basic backoff)
      
      @staticmethod
      def _selectAll(table: str) -> list[dict]:
            if not table:
                  raise ValueError("Table must be provided.")
            
            last_exc: Exception | None = None

            for attemps in range(Sync._MAX_RETRIES):
                  try:
                        response = spb_client.table(table).select("*").execute()
                        
                        if not response or response.data is None:
                              return []
                        
                        return response.data
                  
                  except (TimeoutError, socket.gaierror, ConnectionError) as e:
                        last_exc = e
                        time.sleep(Sync._RETRY_DELAY * (attemps + 1))
                  
                  except:
                        pass
            
            return (spb_client.table(table).select('*').execute())

      @staticmethod
      def fetch_customers() -> list[Customer] | None:
            try:
                  # Check if there is a customer table provided to fetch the customers.
                  if not settings.supabase_table_customers:
                        return None

                  # Fetch customers with supabase client, which will return customer or [] if table is empty, and error if table does'nt exists or RLS.
                  customers = Sync._selectAll(settings.supabase_table_customers)
                  
                  # If customers table is an empty array [] or table does not exists.
                  if not customers:
                        return None

                  # return customers
                  return [Customer(**ctm) for ctm in customers.data]
            
            except Exception as e:
                  print("Fetching Customers Exception: ", e)
                  return None

                  
