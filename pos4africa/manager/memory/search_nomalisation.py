from rapidfuzz import process, fuzz


def search_(name: str, list_: list) -> str | None:
      if not name or not list_:
            print('Customer & Customer list must be provided.')
            return None
      
      best_match = process.extractOne(name, list_, score_cutoff=90)

      return  best_match[0] if best_match else None
