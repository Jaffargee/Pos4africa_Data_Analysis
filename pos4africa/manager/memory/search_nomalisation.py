from rapidfuzz import process, fuzz


def search_customer(customer_name: str, customers: list) -> str | None:
      if not customer_name or not customers:
            print('Customer & Customer list must be provided.')
            return None
      
      best_match = process.extractOne(customer_name, customers, score_cutoff=90)

      return  best_match[0] if best_match else None

def keys2list(obj_dict: dict) -> list:
      if not obj_dict:
            raise ValueError("Object Dict argument is expected.")
      
      keys: list[str] = []
      
      for key, _ in obj_dict.items(): keys.append(key)
      
      return keys