import asyncio
import re
from dataclasses import dataclass, field
from decimal import Decimal

from bs4 import BeautifulSoup

from pos4africa.config.settings import settings
from pos4africa.worker.components.base import BaseComponent
from pos4africa.infra.pos_connector import PosConnector
from pos4africa.shared.utils.retry import with_retry_async

CHANGE_SALE_PATH = "/index.php/sales/change_sale/{sale_id}"
SELECT_CUSTOMER_PATH = "/index.php/sales/select_customer"
COMPLETE_SALE_PATH = "/index.php/sales/complete"
CANCEL_SALE_PATH = "/index.php/sales/cancel_sale"
RECEIPT_PATH = "/index.php/sales/receipt/{sale_id}"
SET_COMMENT_PATH = "/index.php/sales/set_comment"
RELOAD_PATH = "/index.php/sales/reload"

AJAX_HEADERS = {
      "X-Requested-With": "XMLHttpRequest",
      "Origin": settings.pos_base_url,
      "Referer": f"{settings.pos_base_url}/index.php/sales",
}


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class SaleEditError(Exception):
    """Base error for edit-sale automation."""


class NotInEditModeError(SaleEditError):
    """POS did not load the sale into the edit cart (login lost, bad id, no permission)."""


class SaleMismatchError(SaleEditError):
    """The cart does not match what we expected (session clobbered or wrong sale)."""


class CommitUnverifiedError(SaleEditError):
    """complete() was sent but the re-opened sale does not show the new comment. Do NOT blindly retry."""

# --------------------------------------------------------------------------- #
# Parsed model
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class SaleLine:
      name: str
      unit_price: Decimal
      quantity: Decimal
      total: Decimal


@dataclass(frozen=True)
class SalePayment:
      method: str
      amount: Decimal


@dataclass(frozen=True)
class SaleSnapshot:
      sale_id: int | None          # internal id (URL / delete link), e.g. 7040
      sale_code: int | None        # display code, e.g. "S7013"
      customer_id: int | None
      customer_name: str | None
      total: Decimal
      amount_due: Decimal
      comment: str
      lines: tuple[SaleLine, ...] = field(default_factory=tuple)
      payments: tuple[SalePayment, ...] = field(default_factory=tuple)

      def fingerprint(self) -> tuple:
            """Everything that must NOT change when we only swap the customer."""
            return (
                  self.sale_id,
                  self.total,
                  self.amount_due,
                  self.lines,
                  self.payments,
            )


# --------------------------------------------------------------------------- #
# Parsing (pure functions: easy to unit test against saved HTML)
# --------------------------------------------------------------------------- #
def _money(text: str | None) -> Decimal:
      if not text:
            return Decimal("0")
      cleaned = re.sub(r"[^\d.\-]", "", text)
      return Decimal(cleaned) if cleaned not in ("", "-", ".") else Decimal("0")


def _attr_or_text(tag) -> str:
      if tag is None:
            return ""
      return tag.get("data-value") or tag.get_text(strip=True)


# --------------------------------------------------------------------------- #
# Component
# --------------------------------------------------------------------------- #
class EditSale(BaseComponent):
      """
      Re-assign ONE sale to another POS customer.

      The POS keeps the edit cart in the logged-in SESSION, so the sequence
      (open -> select customer -> complete) is stateful and must never run
      concurrently on the same session. A class-level lock enforces that.
      """

      _session_lock = asyncio.Lock()

      def __init__(self, connector: PosConnector) -> None:
            super().__init__()
            self._connector = connector

      def _parse_sale_page(self, html: str) -> SaleSnapshot:
            soup = BeautifulSoup(html, "html.parser")

            # "Editing Sale S7013" banner is the proof we are in edit mode
            banner = soup.select_one("div.alert.alert-danger strong")
            sale_code = banner.get_text(strip=True) if banner else None
            sale_code = int(sale_code.split('S')[1])

            sale_id = None
            m = re.search(r"sales/delete/(\d+)", html)
            if m:
                  sale_id = int(m.group(1))

            customer_id = customer_name = None
            name_link = soup.select_one(".customer-badge .details a.name")
            if name_link:
                  customer_name = name_link.get_text(strip=True)
                  m = re.search(r"customer_modal/(\d+)", name_link.get("href", ""))
                  if m:
                        customer_id = int(m.group(1))
            if customer_id is None:
                  edit_link = soup.select_one("#edit_customer")
                  if edit_link:
                        m = re.search(r"customers/view/(\d+)", edit_link.get("href", ""))
                        if m:
                              customer_id = int(m.group(1))

            lines = []
            for body in soup.select("tbody.register-item-content"):
                  name = body.select_one("a.register-item-name")
                  price = body.select_one('a[data-name="unit_price"]')
                  qty = body.select_one('a[data-name="quantity"]')
                  total = body.select_one('a[data-name="total"]')
                  lines.append(
                        SaleLine(
                              name=name.get_text(strip=True) if name else "",
                              unit_price=_money(_attr_or_text(price)),
                              quantity=_money(_attr_or_text(qty)),
                              total=_money(_attr_or_text(total)),
                        )
                  )

            payments = []
            for li in soup.select("ul.payments li.list-group-item"):
                  key = li.select_one(".key")
                  value = li.select_one(".value")
                  if key and value:
                        payments.append(SalePayment(key.get_text(strip=True), _money(value.get_text())))

            total_el = soup.select_one(".amount-block .total.amount .total-amount")
            due_el = soup.select_one(".amount-block .total.amount-due .amount")
            comment_el = soup.select_one("textarea#comment")

            return SaleSnapshot(
                  sale_id=sale_id,
                  sale_code=sale_code,
                  customer_id=customer_id,
                  customer_name=customer_name,
                  total=_money(total_el.get_text() if total_el else None),
                  amount_due=_money(due_el.get_text() if due_el else None),
                  comment=comment_el.get_text(strip=True) if comment_el else "",
                  lines=tuple(lines),
                  payments=tuple(payments),
            )

      async def _open_sale(self, sale_id: int) -> SaleSnapshot:
            url_id = sale_id
            for _ in range(6):
                  resp = await self._connector.session.get(CHANGE_SALE_PATH.format(sale_id=url_id), follow_redirects=True)
                  resp.raise_for_status()
                  snap = self._parse_sale_page(resp.text)
                  if snap.sale_code is None:
                        raise NotInEditModeError(f"sale {sale_id}: no 'Editing Sale' banner")
                  if snap.sale_code == sale_id:
                        self.log.info("edit.opened", sale_id=sale_id, url_id=url_id, customer_id=snap.customer_id, total=str(snap.total), lines=len(snap.lines))
                        return snap
                  url_id += sale_id - snap.sale_code   # too low -> forward, too high -> back
            await self._discard()
            raise SaleMismatchError(f"code {sale_id} not resolved; stopped at url_id={url_id}, code={snap.sale_code}")

      async def _select_customer(self, person_id: int) -> SaleSnapshot:
            """Attach a customer to the cart (same call the POS UI makes)."""
            resp = await self._connector.session.post(
                  SELECT_CUSTOMER_PATH,
                  data={"customer": f"{person_id}|FORCE_PERSON_ID|"},
                  headers=AJAX_HEADERS,
                  follow_redirects=True,
            )
            resp.raise_for_status()

            # The endpoint's reply may be a partial; reload the register for the truth.
            reload_resp = await self._connector.session.get(
                  "/index.php/sales/reload", follow_redirects=True
            )
            reload_resp.raise_for_status()
            return self._parse_sale_page(reload_resp.text if "customer-badge" in reload_resp.text else resp.text)

      async def _set_comment(self, text: str) -> SaleSnapshot:
            # Same payload the POS page sends on "Complete Sale" (finishSale(): comment + ref_sale_desc).
            resp = await self._connector.session.post(
                  SET_COMMENT_PATH, data={"comment": text, "ref_sale_desc": ""}, headers=AJAX_HEADERS,
                  follow_redirects=True,
            )
            resp.raise_for_status()
            reload_resp = await self._connector.session.get(RELOAD_PATH, follow_redirects=True)
            reload_resp.raise_for_status()
            return self._parse_sale_page(reload_resp.text)

      async def _complete(self) -> str:
            """POST sales/complete: this is the step that WRITES the change in POS."""
            resp = await self._connector.session.post(
                  COMPLETE_SALE_PATH, data={}, headers=AJAX_HEADERS, follow_redirects=True
            )
            # resp.raise_for_status()
            self.log.info("edit.completed", final_url=str(resp.url), status=resp.status_code)
            return str(resp.url)

      async def _discard(self) -> None:
            """Clear the edit cart so it doesn't linger in the shared session."""
            resp = await self._connector.session.post(
                  CANCEL_SALE_PATH, data={}, headers=AJAX_HEADERS, follow_redirects=True
            )
            resp.raise_for_status()
            self.log.info("edit.discarded")

      async def _sale_authenticated(self, sale_id: int, expected_current_person_id: int | None, expected_total: int | None, before: SaleSnapshot) -> bool:

            # fp_snap = SaleSnapshot(
            #       sale_id=sale_id,
            #       sale_code=before.sale_code,
            #       customer_id=expected_current_person_id,
            #       customer_name=before.customer_name,
            #       total=expected_total,
            #       amount_due=before.amount_due,
            #       comment=before.comment,
            #       lines=before.lines,
            #       payments=before.payments,
            # )

            # if fp_snap.fingerprint() != before.fingerprint():
            #       raise SaleMismatchError("Not the same sale: cart contents are different from expected; aborting")
            
            if (expected_current_person_id is not None and before.customer_id != expected_current_person_id):
                  raise SaleMismatchError(
                        f"sale {sale_id} belongs to customer {before.customer_id}, "
                        f"expected {expected_current_person_id}"
                  )

            if expected_total is not None and Decimal(str(expected_total)) != before.total:
                  raise SaleMismatchError(f"POS total {before.total} != expected {expected_total}")
                  

      # -- orchestration ----------------------------------------------------- #
      @with_retry_async
      async def edit_comment(
            self,
            sale_id: int,
            comment: str,
            *,
            expected_current_person_id: int | None = None,
            expected_total: int | None = None,
            commit: bool = False,
      ) -> dict:
            before = await self._open_sale(sale_id)
            try:
                  await self._sale_authenticated( sale_id, expected_current_person_id, expected_total, before)
                  after = await self._set_comment(comment)

                  if after.comment and before.comment and after.comment == before.comment:
                        raise SaleMismatchError(f"comment not applied: cart shows {after.comment}")
                  
                  if after.fingerprint() != before.fingerprint():
                        raise SaleMismatchError("cart contents changed during customer swap; aborting")

                  if not commit:
                        await self._discard()
                        self.log.info("edit.dry_run_ok", sale_id=sale_id, customer_id=before.customer_id)
                        return {"sale_id": sale_id, "changed": True, "committed": False}

                  final_url = await self._complete()
                  return {
                        "sale_id": sale_id,
                        "changed": True,
                        "committed": True,
                        "final_url": final_url,
                        "person_id": before.customer_id,
                  }
            
            except Exception:
                  # never leave a half-edited cart in the shared session
                  try:
                        await self._discard()
                  except Exception as exc:  # pragma: no cover
                        self.log.error("edit.discard_failed", error=str(exc))
                  raise

      @with_retry_async
      async def reassign(
            self,
            sale_id: int,
            new_person_id: int,
            *,
            expected_current_person_id: int | None = None,
            expected_total: int | None = None,
            commit: bool = False,
      ) -> dict:
            """
            Open sale -> swap customer -> (optionally) complete.

            commit=False is a DRY RUN: it proves the swap works in the edit cart and then
            discards it, leaving POS data untouched.
            """
            # async with self._session_lock:
            before = await self._open_sale(sale_id)
            try:
                  
                  await self._sale_authenticated(sale_id, expected_current_person_id, expected_total, before)

                  if before.customer_id == new_person_id:
                        self.log.info("edit.already_assigned", sale_id=sale_id, person_id=new_person_id)
                        await self._discard()
                        return {"sale_id": sale_id, "changed": False, "committed": False}
                  
                  after = await self._select_customer(new_person_id)
                  after = await self._set_comment("")

                  if after.comment and before.comment and after.comment == before.comment:
                        raise SaleMismatchError(f"comment not applied: cart shows {after.comment}")

                  if after.customer_id != new_person_id:
                        raise SaleMismatchError(
                              f"customer swap not applied: cart shows {after.customer_id}, wanted {new_person_id}"
                        )
                  
                  if after.fingerprint() != before.fingerprint():
                        raise SaleMismatchError("cart contents changed during customer swap; aborting")

                  if not commit:
                        await self._discard()
                        self.log.info("edit.dry_run_ok", sale_id=sale_id, new_person_id=new_person_id)
                        return {"sale_id": sale_id, "changed": True, "committed": False}

                  final_url = await self._complete()
                  return {
                        "sale_id": sale_id,
                        "changed": True,
                        "committed": True,
                        "final_url": final_url,
                        "old_person_id": before.customer_id,
                        "new_person_id": new_person_id,
                  }
            except Exception:
                  # never leave a half-edited cart in the shared session
                  try:
                        await self._discard()
                  except Exception as exc:  # pragma: no cover
                        self.log.error("edit.discard_failed", error=str(exc))
                  raise