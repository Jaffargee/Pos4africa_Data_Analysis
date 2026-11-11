from __future__ import annotations

from typing import Protocol

from server.domain.customers.models import CustomerFields


class PosCustomerGateway(Protocol):
    """Talks to POS4Africa.  ``commit=False`` rehearses without changing POS."""

    async def create(self, fields: CustomerFields, *, commit: bool) -> int | None:
        """Create the customer; returns POS ``person_id`` (None when not committed)."""

    async def update(self, pos_customer_id: int, changes: CustomerFields, *, commit: bool) -> int | None:
        """Apply ``changes`` on top of the customer's *current* POS form."""

    async def find(self, fields: CustomerFields) -> int | None:
        """Look the customer up in POS by name/phone.  Used to resolve an unknown
        outcome.  None = provably absent.  Raises PosOutcomeUnknownError if ambiguous."""


class CustomerRepository(Protocol):
    """Supabase side (the analytics copy)."""

    async def search(self, q: str | None, limit: int, offset: int) -> list[dict]: ...

    async def get(self, pos_customer_id: int) -> dict | None: ...

    async def find_by_name(self, first_name: str, last_name: str | None) -> list[dict]: ...

    async def save_from_pos(self, pos_customer_id: int, fields: dict) -> None:
        """Idempotent upsert keyed on pos_customer_id; only the given fields change."""
