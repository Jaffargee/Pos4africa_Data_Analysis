from pos4africa.infra.supabase_client import spb_client
# from pos4africa.config import settings
from pos4africa.shared.models.customer import Customer
from pos4africa.shared.models.account import Account

# print(settings.supabase_url, settings.supabase_service_key)


class Sync:

      @staticmethod
      def fetch_customers() -> list[Customer] | None:
            try:
  
                  # Check for intenet connection first before making the request.

                  # Check if there is a customer table provided to fetch the customers.
                  # if not settings.supabase_table_customers:
                  #       pass

                  # Fetch customers with supabase client, which will return customer or [] if table is empty, and error if table does'nt exists or RLS.
                  # customers = spb_client.table(settings.supabase_table_customers).select('*').execute()
                  customers = (spb_client.table('customers').select('*').execute())
                  
                  # If customers table is an empty array [] or table does not exists.
                  if not customers:
                        pass

                  # return customers
                  return [Customer(**ctm) for ctm in customers.data]
            
            except Exception as e:
                  return None
                  
      @staticmethod
      def fetch_accounts() -> list[Account] | None:
            try:
  
                  # Check for intenet connection first before making the request.

                  # Check if there is a customer table provided to fetch the customers.
                  # if not settings.supabase_table_accounts:
                  #       pass

                  # Fetch customers with supabase client, which will return customer or [] if table is empty, and error if table does'nt exists or RLS.
                  # accounts = spb_client.table(settings.supabase_table_accounts).select('*').execute()
                  accounts = (spb_client.table('accounts').select('*').execute())
                  
                  # If customers table is an empty array [] or table does not exists.
                  if not accounts:
                        pass

                  # return accounts
                  return [Account(**acct) for acct in accounts.data]
            
            except Exception as e:
                  return None
                  
