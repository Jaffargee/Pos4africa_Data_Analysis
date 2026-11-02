import json
from pathlib import Path
import os
from pos4africa.shared.models.customer import Customer
import hashlib
from typing import Sequence, Mapping
from pydantic import BaseModel, ConfigDict

class MatchedResults(BaseModel):
      model_config = ConfigDict(frozen=True)
      comment: str
      count: int
      pos_customer_id: int | None
      customer_found: bool
      

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

      def sync(self, customers: list[Customer]) -> dict:

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

      def match_comments_to_customers(
            self, 
            comments: Sequence[dict], 
            customers: Mapping[str, Mapping]
      ) -> Sequence[MatchedResults]:
            return self.__match_comments_to_customers(comments=comments, customers=customers)
            
      def __match_comments_to_customers(
            self, 
            comments: Sequence[dict], 
            customers: Mapping[str, Mapping]
      ) -> Sequence[MatchedResults]:

            if not comments or not customers:
                  return []

            if not isinstance(comments, Sequence):
                  raise TypeError("`comments` must be a sequence of dictionaries.")

            if not isinstance(customers, Mapping):
                  raise TypeError("`customers` must be a dictionary-like mapping.")
            
            customer_id_map: Mapping[str, int] = {}
            results: Sequence[MatchedResults] = []

            for info in customers.values():
                  first = info.get("first_name") or ""
                  last = info.get("last_name") or ""
                  
                  fullname = f"{first} {last}".strip().upper()

                  if fullname: customer_id_map[fullname] = info["pos_customer_id"]

            for item in comments:
                  if not "comment" in item or not isinstance(item["comment"], str): continue

                  comment: str = item["comment"].strip().upper()

                  matched_found_id = customer_id_map.get(comment, None)

                  results.append(MatchedResults(
                        comment=comment,
                        count=item.get("count", 0),
                        pos_customer_id=matched_found_id,
                        customer_found=matched_found_id is not None
                  ))

            return results
