from __future__ import annotations

import asyncio

from server.infrastructure.supabase._client import get_client

_COLUMNS = "pos_item_id,item_name,category,selling_price,cost_price,quantity,inactive"


class SupabaseItemRepository:
    """Read-only item catalogue (the dashboard searches items in Supabase directly)."""

    def __init__(self, client_factory=get_client):
        self._client = client_factory

    async def get_many(self, ids: list[int]) -> dict[int, dict]:
        def _do():
            rows = self._client().table("items").select(_COLUMNS).in_("pos_item_id", sorted(set(ids))).execute().data
            return {r["pos_item_id"]: r for r in rows}

        return await asyncio.to_thread(_do)
