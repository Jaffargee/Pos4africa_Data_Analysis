from __future__ import annotations

import re
from typing import Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

_WS = re.compile(r"\s+")


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = _WS.sub(" ", value).strip()
    return cleaned or None


class CustomerFields(BaseModel):
    """What can be said about a customer.  Everything optional; subclasses tighten it."""

    model_config = ConfigDict(extra="forbid")

    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone_number: str | None = None
    state: str | None = None
    country: str | None = None

    @field_validator("first_name", "last_name", "email", "state", "country", mode="before")
    @classmethod
    def _strip(cls, v):
        return _clean(v) if isinstance(v, str) or v is None else v

    @field_validator("phone_number", mode="before")
    @classmethod
    def _phone(cls, v):
        if v is None:
            return None
        digits = re.sub(r"[^\d+]", "", str(v))
        if not digits:
            return None
        if not 7 <= len(digits.lstrip("+")) <= 15:
            raise ValueError("phone_number must have 7-15 digits")
        return digits

    @field_validator("email")
    @classmethod
    def _email(cls, v):
        if v is not None and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", v):
            raise ValueError("email is not a valid address")
        return v

    def pos_fields(self) -> dict[str, str]:
        """Only the fields that carry a value, keyed the way the POS form names them."""
        return {k: v for k, v in self.model_dump().items() if v}


class CustomerCreateRequest(CustomerFields):
    first_name: str  # type: ignore[assignment]
    allow_duplicate: bool = False

    @field_validator("first_name")
    @classmethod
    def _required(cls, v):
        if not v:
            raise ValueError("first_name is required")
        return v

    def to_fields(self) -> CustomerFields:
        return CustomerFields(**self.model_dump(exclude={"allow_duplicate"}))


class CustomerUpdateRequest(CustomerFields):
    @model_validator(mode="after")
    def _not_empty(self) -> Self:
        if not self.pos_fields():
            raise ValueError("provide at least one field to change")
        return self
