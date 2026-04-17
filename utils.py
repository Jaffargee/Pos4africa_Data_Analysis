import re
import pandas as pd

def parse_sale_id(sale_id):
      if pd.isna(sale_id):
            return None

      sale_id = str(sale_id).strip()

      # Extract digits (handles S107, S107 Edit, etc.)
      match = re.search(r"\d+", sale_id)

      if match:
            return int(match.group())
      
      return None