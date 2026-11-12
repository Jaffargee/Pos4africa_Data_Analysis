from __future__ import annotations

import asyncio

from server.infrastructure.supabase._client import get_client, safe_term

_COLUMNS = "id,pos_customer_id,first_name,last_name,customer_name,company_name,state,city,country,category,status_level,is_active"


class SupabaseCustomerRepository:
    """Read-only.  Customers reach Supabase only through the ETL."""

    def __init__(self, client_factory=get_client):
        self._client = client_factory

    async def get(self, pos_customer_id: int) -> dict | None:
        def _do():
            rows = (
                self._client().table("customers").select(_COLUMNS)
                .eq("pos_customer_id", pos_customer_id).limit(1).execute().data
            )
            return rows[0] if rows else None

        return await asyncio.to_thread(_do)

    async def find_by_name(self, first_name: str, last_name: str | None) -> list[dict]:
        first, last = safe_term(first_name), safe_term(last_name)
        if first is None:
            return []

        def _do():
            q = self._client().table("customers").select(_COLUMNS).ilike("first_name", first)
            q = q.ilike("last_name", last) if last else q.is_("last_name", "null")
            return q.limit(5).execute().data

        return await asyncio.to_thread(_do)
