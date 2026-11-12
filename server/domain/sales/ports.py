from __future__ import annotations

from datetime import datetime
from typing import Protocol
from uuid import UUID

from server.domain.sales.models import FoundSale, ResolvedSale


class PosSalesGateway(Protocol):
    async def create(self, sale: ResolvedSale, *, commit: bool) -> None:
        """Build the cart in POS and (when ``commit``) complete it.  The POS sale
        code is *not* returned here; it is read back with ``find_by_ref``."""

    async def find_by_ref(self, ref: str, *, since: datetime) -> FoundSale | None:
        """Find the sale whose POS comment carries ``ref``.  None = provably absent."""


class AccountRepository(Protocol):
    """Read-only: payment accounts as the ETL has loaded them into Supabase."""

    async def get_accounts(self, ids: list[UUID]) -> dict[UUID, str]: ...


class ItemRepository(Protocol):
    """Read-only: item catalogue (prices, active flag) from Supabase."""

    async def get_many(self, ids: list[int]) -> dict[int, dict]: ...
