from __future__ import annotations

import asyncio
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from pos4africa.config.settings import settings as pos_settings
from pos4africa.shared.utils.sale_ref import extract_ref
from pos4africa.worker.components.extractors.sales import SaleExtractor
from pos4africa.worker.components.scrapers.sales.edit_sale import (
    AJAX_HEADERS,
    CANCEL_SALE_PATH,
    COMPLETE_SALE_PATH,
    RELOAD_PATH,
    SELECT_CUSTOMER_PATH,
    SET_COMMENT_PATH,
)

from server.config import ServerSettings
from server.domain.errors import (
    PosOutcomeUnknownError,
    PosRejectedError,
    PosSessionExpiredError,
    PosUnavailableError,
)
from server.domain.sales.models import FoundSale, ResolvedSale
from server.infrastructure.pos4africa.errors import classify_http_error, classify_pre_commit_error
from server.infrastructure.pos4africa.html import parse_cart, verify_cart
from server.infrastructure.pos4africa.session_pool import PosSessionPool


def _is_login_page(resp) -> bool:
    return pos_settings.pos_login_path in str(resp.url)


class PosSalesGatewayImpl:
    """Creates sales through the POS register, the same way a cashier would.

    UNVERIFIED against a live POS: the add-item / edit-item / add-payment calls (paths in
    ServerSettings).  Safety nets that make a wrong guess harmless:
      * every cart step is followed by re-reading the cart; the sale is only completed
        if the cart is *exactly* the intended sale (items, prices, total, payments, ref);
      * dry_run mode builds the cart, verifies it, and discards it.
    """

    def __init__(self, pool: PosSessionPool, cfg: ServerSettings):
        self._pool = pool
        self._cfg = cfg

    # -- create -----------------------------------------------------------------------
    async def create(self, sale: ResolvedSale, *, commit: bool) -> None:
        async with self._pool.acquire() as conn:
            s = conn.session
            try:
                await self._post(s, CANCEL_SALE_PATH)  # clear any leftover cart
                await self._post(s, SELECT_CUSTOMER_PATH, {"customer": f"{sale.pos_customer_id}|FORCE_PERSON_ID|"})
                for n, line in enumerate(sale.lines, start=1):
                    await self._post(s, self._cfg.pos_sales_add_item_path, {"item": str(line.pos_item_id)})
                    edit = self._cfg.pos_sales_edit_item_path.format(line=n)
                    await self._post(s, edit, {"name": "quantity", "value": str(line.quantity)})
                    await self._post(s, edit, {"name": "price", "value": str(line.unit_price)})
                for pay in sale.payments:
                    await self._post(
                        s, self._cfg.pos_sales_add_payment_path,
                        {"payment_type": pay.account, "amount_tendered": str(pay.amount)},
                    )
                await self._post(s, SET_COMMENT_PATH, {"comment": sale.comment_with_ref, "ref_sale_desc": ""})
                reloaded = await s.get(RELOAD_PATH, follow_redirects=True)
                reloaded.raise_for_status()
                if _is_login_page(reloaded):
                    raise PosSessionExpiredError("POS session expired while building the cart")
                verify_cart(parse_cart(reloaded.text), sale)
            except Exception as exc:
                await self._cancel_quietly(s)
                raise classify_pre_commit_error(exc) from exc

            if not commit:
                await self._cancel_quietly(s)
                return

            # Point of no return: from here a failure means "might have been applied".
            try:
                resp = await s.post(COMPLETE_SALE_PATH, data={}, headers=AJAX_HEADERS, follow_redirects=True)
            except Exception as exc:
                raise classify_http_error(exc) from exc
            if _is_login_page(resp):
                raise PosSessionExpiredError("POS session expired before the sale was completed")
            if resp.status_code >= 500:
                raise PosOutcomeUnknownError(f"POS answered HTTP {resp.status_code} while completing the sale")
            if resp.status_code >= 400:
                raise PosRejectedError(f"POS refused to complete the sale (HTTP {resp.status_code})")

    async def _post(self, session, path: str, data: dict | None = None) -> None:
        resp = await session.post(path, data=data or {}, headers=AJAX_HEADERS, follow_redirects=True)
        resp.raise_for_status()
        if _is_login_page(resp):
            raise PosSessionExpiredError("POS session expired while building the cart")

    async def _cancel_quietly(self, session) -> None:
        try:
            await session.post(CANCEL_SALE_PATH, data={}, headers=AJAX_HEADERS, follow_redirects=True)
        except Exception:  # best effort: the next create starts with a cancel anyway
            pass

    # -- find_by_ref --------------------------------------------------------------------
    async def find_by_ref(self, ref: str, *, since: datetime) -> FoundSale | None:
        age = datetime.now(timezone.utc) - since
        date_range = "TODAY" if age < timedelta(hours=12) else "ALL_TIME"
        path = self._cfg.pos_sales_report_path.format(range=date_range)

        async with self._pool.acquire() as conn:
            try:
                resp = await conn.session.get(path, follow_redirects=True)
                resp.raise_for_status()
            except Exception as exc:
                raise classify_http_error(exc) from exc
        if not resp.content.startswith(b"PK"):
            raise PosOutcomeUnknownError("POS sales report was not an Excel file (session expired?)")

        def _parse():
            with tempfile.TemporaryDirectory() as tmp:
                p = Path(tmp) / "sales.xlsx"
                p.write_bytes(resp.content)
                return SaleExtractor()._extract(p)

        raw_sales = await asyncio.to_thread(_parse)
        hits = [s for s in raw_sales if extract_ref(s.comment) == ref.upper()]
        if not hits:
            return None
        if len(hits) > 1:
            raise PosOutcomeUnknownError(f"{len(hits)} POS sales carry ref {ref}")
        raw = hits[0]
        return FoundSale(
            pos_sale_id=int(raw.pos_sale_id),
            pos_customer_id=int(raw.pos_customer_id) if raw.pos_customer_id else self._cfg.walk_in_customer_id,
            invoice_total=Decimal(str(raw.invoice_total or "0").replace(",", "")),
            lines=tuple(
                (int(i.pos_item_id), Decimal(str(i.quantity)), Decimal(str(i.unit_price).replace(",", "")))
                for i in raw.items if i.pos_item_id
            ),
            comment=raw.comment,
        )
