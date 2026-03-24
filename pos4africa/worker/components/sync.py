from pos4africa.infra.supabase_client import spb_client
from pos4africa.config import settings
from pos4africa.shared.models.customer import Customer
from pos4africa.shared.models.account import Account


class Sync:
      
      @staticmethod
      def _selectAll(table: str) -> list[dict] | None:
            if not table:
                  print('Table must be provided.')
                  return None
            
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

                  
