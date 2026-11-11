from pydantic import BaseModel, ConfigDict


class PosCustomerPayload(BaseModel):
      id: int | None
      first_name: str
      last_name: str
      email: str | None
      phone_number: str | None
      additional_phone_numbers: list[str] | None
      state: str


class CustomerRequest(BaseModel):
      model_config = ConfigDict(extra="forbid")
      first_name: str
      last_name: str
      email: str | None = None
      phones: list[str] | None = None
      state: str | None = None