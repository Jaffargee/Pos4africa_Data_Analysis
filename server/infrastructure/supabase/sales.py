from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID

from server.domain.sales.models import ResolvedSale
from server.infrastructure.supabase._client import get_client

_SALE_COLUMNS = "id,pos_sale_id,pos_customer_id,customer_name,salesperson,comment,invoice_total,items_sold,invoice_datetime"


class SupabaseSaleRepository:
    def __init__(self, client_factory=get_client):
        self._client = client_factory

    async def list(self, pos_customer_id: int | None, limit: int, offset: int) -> list[dict]:
        def _do():
            q = self._client().table("sales").select(_SALE_COLUMNS).order("invoice_datetime", desc=True)
            if pos_customer_id is not None:
                q = q.eq("pos_customer_id", pos_customer_id)
            return q.range(offset, offset + limit - 1).execute().data

        return await asyncio.to_thread(_do)

    async def get(self, pos_sale_id: int) -> dict | None:
        def _do():
            c = self._client()
            rows = c.table("sales").select(_SALE_COLUMNS).eq("pos_sale_id", pos_sale_id).limit(1).execute().data
            if not rows:
                return None
            sale = rows[0]
            sale["items"] = c.table("sale_items").select("pos_item_id,name,quantity,unit_price,total").eq("pos_sale_id", pos_sale_id).execute().data
            sale["payments"] = c.table("payments").select("account,amount").eq("pos_sale_id", pos_sale_id).execute().data
            return sale

        return await asyncio.to_thread(_do)

    async def get_accounts(self, ids: list[UUID]) -> dict[UUID, str]:
        def _do():
            rows = self._client().table("accounts").select("id,name").in_("id", [str(i) for i in ids]).execute().data
            return {UUID(r["id"]): r["name"] for r in rows}

        return await asyncio.to_thread(_do)

    async def commit_sale(self, pos_sale_id: int, sale: ResolvedSale) -> bool:
        params = {
            "p_pos_sale_id": pos_sale_id,
            "p_pos_customer_id": sale.pos_customer_id,
            "p_customer_name": sale.customer_name,
            "p_salesperson": sale.salesperson,
            "p_comment": sale.comment,  # clean comment: the ref never reaches analytics
            "p_invoice_datetime": datetime.now(timezone.utc).isoformat(),  # provisional; ETL overwrites
            "p_lines": [
                {"pos_item_id": l.pos_item_id, "name": l.name, "quantity": str(l.quantity),
                 "unit_price": str(l.unit_price), "total": str(l.total)}
                for l in sale.lines
            ],
            "p_payments": [
                {"account_id": str(p.account_id), "account": p.account, "amount": str(p.amount)}
                for p in sale.payments
            ],
        }
        res = await asyncio.to_thread(lambda: self._client().rpc("commit_pos_sale", params).execute())
        return bool((res.data or {}).get("inserted"))
