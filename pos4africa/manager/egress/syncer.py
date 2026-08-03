import json
from pathlib import Path
import os
from pos4africa.shared.models.customer import Customer
import hashlib

"""
Object File Blueprint

{

'int(pos_customer_id)': {
      pos_customer_id: int,
      first_name: str,
      last_name: str,
      hash: str
}
}

"""


class Syncer:

      def __init__(self):
            self.customers_local_db_file = "customers.sync.json"
            self.customers_local_db_file_path = Path(self.customers_local_db_file).resolve()
            self.ensure_file_exists(self.customers_local_db_file_path)

            self.customers_db = (
                  self.load_json_object(self.customers_local_db_file_path)
                  if os.path.exists(self.customers_local_db_file_path)
                  else {}
            )

      def get_customers(self) -> dict:
            return self.customers_db

      def ensure_file_exists(self, file_path: str) -> None:
            path = Path(file_path)
            if not path.exists():
                  path.write_text("{}")  # Seed with valid empty JSON list instead of empty file

            return None

      def load_json_object(self, file_path: str) -> dict:
            if not os.path.exists(file_path):
                  raise FileNotFoundError(f"Json Object File not found: {file_path}")

            # If the file is empty (0 bytes), return default empty list
            if os.path.getsize(file_path) == 0:
                  return []

            with open(file_path, "r") as file:
                  try:
                        data = json.load(file)
                        return data
                  except json.JSONDecodeError:
                        # Fallback if the file got corrupted
                        return []

      def save_json_object(self, file_path: str, data: dict = None) -> None:
            if data is None:
                  data = {}

            # Check if files exits before writing to it
            if not os.path.exists(file_path):
                  raise FileNotFoundError(f"Json Object File not found: {file_path}")
            
            sorted_keys = sorted(data.keys(), key=lambda k: int(k) if str(k).isdigit() else k)
            sorted_data = {k: data[k] for k in sorted_keys}
            with open(file_path, "w") as file:
                  json.dump(sorted_data, file, indent=4)

      def sync(self, customers: list[Customer]) -> None:

            # Ensure the file exists
            self.ensure_file_exists(self.customers_local_db_file_path)

            customers_to_sync = {}

            for customer in customers:
                  # Check if the customer data changes then we update the local db file, we check the hash againts hash customer data
                  customer_hash = self.hash_customer_data(customer)
                  ctm_id_str = str(customer.pos_customer_id)

                  customer_payload = {
                        "pos_customer_id": customer.pos_customer_id,
                        "first_name": customer.first_name,
                        "last_name": customer.last_name,
                        "hash": customer_hash
                  }

                  existing_customer = self.customers_db.get(ctm_id_str)

                  if existing_customer:
                        if existing_customer["hash"] != customer_hash:
                              # Update the existing customer data
                              customers_to_sync[ctm_id_str] = customer_payload
                              # Update the local db
                        else:
                              # No changes, skip syncing
                              continue
                  else:
                        # New customer, add to sync
                        customers_to_sync[ctm_id_str] = customer_payload

            if not customers_to_sync:
                  return {}

            self.customers_db.update(customers_to_sync)

            return customers_to_sync

      def normalize_customer_data(self, ctms: dict) -> dict:
            customers_normalized = []
            for customer in ctms.values():
                  # Copy the dict so we don't mutate self.customers_db in memory
                  clean_customer = {k: v for k, v in customer.items() if k != "hash"}
                  customers_normalized.append(clean_customer)
            
            return customers_normalized

      def hash_customer_data(self, customer: Customer) -> str:    
            # Create a unique string representation of the customer data
            customer_data_str = f"{customer.pos_customer_id}-{customer.first_name}-{customer.last_name}"
            # Generate a hash of the customer data
            return hashlib.sha256(customer_data_str.encode()).hexdigest()