
from __future__ import annotations

import hashlib

def sale_fingerprint(pos_sale_id: str) -> str:
      return hashlib.sha256(pos_sale_id.encode()).hexdigest()
