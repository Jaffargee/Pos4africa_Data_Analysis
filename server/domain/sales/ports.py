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


class SaleRepository(Protocol):
    async def list(self, pos_customer_id: int | None, limit: int, offset: int) -> list[dict]: ...

    async def get(self, pos_sale_id: int) -> dict | None: ...

    async def get_accounts(self, ids: list[UUID]) -> dict[UUID, str]: ...

    async def commit_sale(self, pos_sale_id: int, sale: ResolvedSale) -> bool:
        """Atomically write sale + items + payments under the *POS* id.
        Idempotent: returns False (and changes nothing) if the sale already exists."""


class ItemRepository(Protocol):
    async def get_many(self, ids: list[int]) -> dict[int, dict]: ...

    async def search(self, q: str | None, limit: int) -> list[dict]: ...
