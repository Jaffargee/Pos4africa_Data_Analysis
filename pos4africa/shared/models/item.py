from pydantic import BaseModel, Field, EmailStr
from uuid import UUID
from datetime import datetime
from decimal import Decimal

class Item(BaseModel):
      id: UUID | None = None

      pos_item_id: int
      
      item_name: str | None = None
      category: str | None = None

      cost_price: Decimal
      selling_price: Decimal

      quantity: float

      is_barcoded: bool
      
      created_at: datetime | None = None
