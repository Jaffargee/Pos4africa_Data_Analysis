from __future__ import annotations

import asyncio
import re
import tempfile
from pathlib import Path

from pos4africa.config.settings import settings as pos_settings
from pos4africa.worker.components.extractors.customers import CustomerExtractor
from pos4africa.worker.components.scrapers.customers.create_customer import DEFAULT_PAYLOAD, HEADERS

from server.config import ServerSettings
from server.domain.customers.models import CustomerFields
from server.domain.errors import PosOutcomeUnknownError, PosRejectedError, PosSessionExpiredError
from server.infrastructure.pos4africa.errors import classify_http_error
from server.infrastructure.pos4africa.html import parse_form_fields
from server.infrastructure.pos4africa.session_pool import PosSessionPool


def _digits(value: str | None) -> str:
    return re.sub(r"\D", "", value or "")


def _is_login_page(resp) -> bool:
    return pos_settings.pos_login_path in str(resp.url)


class PosCustomerGatewayImpl:
    """Customer writes against POS4Africa.  ``create`` reuses the exact form payload the
    ETL's CreateCustomer already sends successfully; ``update`` reads the customer's
    current form first so fields we were not told about are never blanked."""

    def __init__(self, pool: PosSessionPool, cfg: ServerSettings):
        self._pool = pool
        self._cfg = cfg

    # -- create -----------------------------------------------------------------------
    async def create(self, fields: CustomerFields, *, commit: bool) -> int | None:
        if not commit:
            return None  # nothing to rehearse: a customer has no cart to discard
        data = {**DEFAULT_PAYLOAD, **fields.pos_fields()}
        return await self._save(pos_settings.pos_customer_form_action, data)

    # -- update -----------------------------------------------------------------------
    async def update(self, pos_customer_id: int, changes: CustomerFields, *, commit: bool) -> int | None:
        async with self._pool.acquire() as conn:
            try:
                resp = await conn.session.get(f"/index.php/customers/view/{pos_customer_id}", follow_redirects=True)
                resp.raise_for_status()
            except Exception as exc:
                raise classify_http_error(exc) from exc
            if _is_login_page(resp):
                raise PosSessionExpiredError("POS session expired")
            current = parse_form_fields(resp.text)

        # Never POST a form we could not read: the POS would blank every field we left out.
        if not current.get("first_name"):
            raise PosRejectedError(f"Could not read the current POS form for customer {pos_customer_id}")
        if not commit:
            return None
        data = {**DEFAULT_PAYLOAD, **current, **changes.pos_fields()}
        endpoint = pos_settings.pos_customer_edit_form_action.format(person_id=pos_customer_id)
        person_id = await self._save(endpoint, data)
        if person_id != pos_customer_id:
            raise PosOutcomeUnknownError(f"POS answered person_id {person_id} while updating {pos_customer_id}")
        return person_id

    async def _save(self, endpoint: str, data: dict) -> int:
        async with self._pool.acquire() as conn:
            try:
                resp = await conn.session.post(endpoint, data=data, headers=HEADERS)
                resp.raise_for_status()
            except Exception as exc:
                raise classify_http_error(exc) from exc
            if _is_login_page(resp):
                raise PosSessionExpiredError("POS session expired before the write was accepted")
            try:
                body = resp.json()
            except ValueError as exc:
                raise PosOutcomeUnknownError("POS returned a non-JSON reply to a customer save") from exc
        try:
            person_id = int(body.get("person_id") or -1)
        except (TypeError, ValueError):
            person_id = -1
        if not body.get("success") or person_id <= 0:
            raise PosRejectedError(body.get("message") or "POS refused to save the customer")
        return person_id

    # -- find (outcome resolution) ----------------------------------------------------
    async def find(self, fields: CustomerFields) -> int | None:
        async with self._pool.acquire() as conn:
            try:
                resp = await conn.session.get(self._cfg.pos_customers_export_path, follow_redirects=True)
                resp.raise_for_status()
            except Exception as exc:
                raise classify_http_error(exc) from exc
        if not resp.content.startswith(b"PK"):
            raise PosOutcomeUnknownError("POS customer export was not an Excel file (session expired?)")

        def _parse():
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / "customers.xlsx"
                path.write_bytes(resp.content)
                return CustomerExtractor()._extract(path)

        customers = await asyncio.to_thread(_parse)
        want_first = (fields.first_name or "").casefold()
        want_last = (fields.last_name or "").casefold()
        want_phone = _digits(fields.phone_number)

        matches = [
            c for c in customers
            if (c.first_name or "").strip().casefold() == want_first
            and (c.last_name or "").strip().casefold() == want_last
            and (not want_phone or _digits(c.phone_number) == want_phone)
        ]
        if not matches:
            return None
        if len(matches) > 1:
            raise PosOutcomeUnknownError(
                f"{len(matches)} POS customers match this name; cannot tell which one this request created"
            )
        return matches[0].pos_customer_id
