from pydantic import BaseModel, Field, EmailStr
from uuid import UUID
from datetime import datetime
from decimal import Decimal

class Customer(BaseModel):
      id: UUID | None = None

      pos_customer_id: int | None
      
      first_name: str | None = None
      last_name: str | None = None
      phone_number: str | None = None
      state: str | None = None
      
      created_at: datetime | None = None
