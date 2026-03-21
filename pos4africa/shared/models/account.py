from pydantic import BaseModel
from uuid import UUID
from decimal import Decimal
from datetime import datetime


class Account(BaseModel):
      id: UUID | None = None

      account_no: int
      account_name: str
      account_bank: str

      currency: str = 'NGN'
      active: bool = True

      balance: Decimal = Decimal("0.00")
    
      created_at: datetime | None = None