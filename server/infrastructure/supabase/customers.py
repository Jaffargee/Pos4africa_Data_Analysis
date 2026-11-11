from __future__ import annotations

import asyncio

from server.infrastructure.supabase._client import get_client, safe_term

_COLUMNS = "id,pos_customer_id,first_name,last_name,customer_name,company_name,state,city,country,category,status_level,is_active"
_SAVABLE = {"first_name", "last_name", "state", "country"}  # columns that exist on `customers` today


class SupabaseCustomerRepository:
    def __init__(self, client_factory=get_client):
        self._client = client_factory

    async def search(self, q: str | None, limit: int, offset: int) -> list[dict]:
        term = safe_term(q)

        def _do():
            query = self._client().table("customers").select(_COLUMNS).order("customer_name")
            if term:
                query = query.or_(f"customer_name.ilike.%{term}%,company_name.ilike.%{term}%")
            return query.range(offset, offset + limit - 1).execute().data

        return await asyncio.to_thread(_do)

    async def get(self, pos_customer_id: int) -> dict | None:
        def _do():
            rows = self._client().table("customers").select(_COLUMNS).eq("pos_customer_id", pos_customer_id).limit(1).execute().data
            return rows[0] if rows else None

        return await asyncio.to_thread(_do)

    async def find_by_name(self, first_name: str, last_name: str | None) -> list[dict]:
        def _do():
            q = self._client().table("customers").select(_COLUMNS).ilike("first_name", first_name)
            q = q.ilike("last_name", last_name) if last_name else q.is_("last_name", "null")
            return q.limit(5).execute().data

        return await asyncio.to_thread(_do)

    async def save_from_pos(self, pos_customer_id: int, fields: dict) -> None:
        row = {k: v for k, v in fields.items() if k in _SAVABLE and v}
        # phone/email live in POS only; the next ETL run brings in everything else.

        def _do():
            c = self._client()
            # Plain UPDATE first: an upsert that omits a NOT NULL column (first_name) fails
            # in Postgres even when the row exists.
            if row:
                updated = c.table("customers").update(row).eq("pos_customer_id", pos_customer_id).execute().data
                if updated:
                    return
            elif c.table("customers").select("id").eq("pos_customer_id", pos_customer_id).limit(1).execute().data:
                return
            if "first_name" not in row:
                return  # not mirrored yet and nothing to insert with; the ETL will load it
            c.table("customers").upsert({**row, "pos_customer_id": pos_customer_id}, on_conflict="pos_customer_id").execute()

        await asyncio.to_thread(_do)
