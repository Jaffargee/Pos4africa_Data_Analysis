from pydantic import BaseModel, Field, EmailStr
from uuid import UUID
from datetime import datetime
from decimal import Decimal

class Customer(BaseModel):
      id: UUID | None = None

      pos_customer_id: int
      name: str

      first_name: str | None = None
      last_name: str | None = None
      company_name: str | None = None

      email: EmailStr | None = None
      phone: str | None = None

      balance: Decimal = Decimal("0.00")
      account_no: str | None = None
      comments: str | None = None

      credit_limit: Decimal = Decimal("0.00")
      disable_loyalty: bool = False
      points: int = 0

      address1: str | None = None
      address2: str | None = None
      city: str | None = None
      state: str | None = None
      zip: str | None = None
      country: str | None = None

      created_at: datetime | None = None