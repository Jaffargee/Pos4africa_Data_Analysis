from __future__ import annotations

import asyncio
from uuid import UUID

from server.infrastructure.supabase._client import get_client


class SupabaseAccountRepository:
    """Read-only: payment accounts as loaded by the ETL."""

    def __init__(self, client_factory=get_client):
        self._client = client_factory

    async def get_accounts(self, ids: list[UUID]) -> dict[UUID, str]:
        def _do():
            rows = self._client().table("accounts").select("id,name").in_("id", [str(i) for i in ids]).execute().data
            return {UUID(r["id"]): r["name"] for r in rows}

        return await asyncio.to_thread(_do)
