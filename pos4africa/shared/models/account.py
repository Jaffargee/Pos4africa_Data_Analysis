from pydantic import BaseModel
from uuid import UUID
from decimal import Decimal
from datetime import datetime


class Account(BaseModel):
      id: UUID | None = None

      account_no: int | None = None
      account_name: str | None = None
      account_bank: str | None = None

      currency: str = 'NGN'
      active: bool = True

      balance: Decimal | None = Decimal("0.00") 
    
      created_at: datetime | None = None