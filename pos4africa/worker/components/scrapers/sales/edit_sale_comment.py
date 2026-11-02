"""
Rewrite the COMMENT of one POS sale (e.g. CTM -> CUSTOMER) through the POS edit-sale flow.

Place next to edit_sale_customer.py (adjust the import below if yours lives elsewhere).

Safety rules baked in:
  * never edits a sale whose displayed code != the code we were asked for
  * optional guard: only edits if the POS comment equals what Supabase says it was
  * cart contents (lines / payments / totals) must be identical after the comment swap
  * dry run (commit=False) discards the cart and leaves POS untouched
  * after a commit the sale is re-opened and re-checked (verify=True)
  * the lock is PER SESSION (instance), so N workers with N sessions run in parallel
"""
from __future__ import annotations

import asyncio
from decimal import Decimal

from pos4africa.infra.pos_connector import PosConnector
from pos4africa.worker.components.base import BaseComponent
from pos4africa.worker.components.scrapers.sales.edit_sale import (  # adjust path if needed
    AJAX_HEADERS,
    CANCEL_SALE_PATH,
    CHANGE_SALE_PATH,
    COMPLETE_SALE_PATH,
    NotInEditModeError,
    SaleEditError,
    SaleMismatchError,
    SaleSnapshot,
    parse_sale_page,
)

# Confirmed from the real edit page: the UI posts {comment} to sales/set_comment, then reloads the register.
SET_COMMENT_PATH = "/index.php/sales/set_comment"
RELOAD_PATH = "/index.php/sales/reload"


class CommitUnverifiedError(SaleEditError):
    """complete() was sent but the re-opened sale does not show the new comment. Do NOT blindly retry."""


def norm(text: str | None) -> str:
    """Comparison form: collapse whitespace, ignore case."""
    return " ".join((text or "").split()).casefold()


def _content_fp(s: SaleSnapshot) -> tuple:
    """Everything that must NOT change when only the comment changes."""
    return (s.total, s.amount_due, s.lines, s.payments)


def _safe_parse(html: str) -> SaleSnapshot:
      try:
            return parse_sale_page(html)
      except (AttributeError, IndexError, ValueError) as exc:
            # parse_sale_page crashes when the "Editing Sale" banner is missing
            raise NotInEditModeError(f"page is not an edit-sale page ({exc!r})") from exc


class EditSaleComment(BaseComponent):
      def __init__(self, connector: PosConnector) -> None:
            super().__init__()
            self._c = connector
            self._lock = asyncio.Lock()  # one edit cart per session -> serialize per session only

      # -- steps ------------------------------------------------------------- #
      async def _get(self, url_id: int) -> SaleSnapshot:
            resp = await self._c.session.get(CHANGE_SALE_PATH.format(sale_id=url_id), follow_redirects=True)
            resp.raise_for_status()
            return _safe_parse(resp.text)

      async def open_sale(self, wanted: int, id_kind: str = "code") -> SaleSnapshot:
            """
            Load a sale into the edit cart and PROVE it is the one asked for.

            id_kind="code":     wanted is the display code ("Editing Sale S7441" banner).
                              The URL id differs from the code (store-account payments take ids), so we
                              converge: next_url = snap.sale_id + (wanted - snap.sale_code), a few hops max.
            id_kind="internal": wanted is the internal id used in the URL / delete link (sales/delete/7470).
            """
            seen: set[int] = set()
            url_id = wanted
            probed = False
            try:
                  for _ in range(6):
                        seen.add(url_id)
                        snap = await self._get(url_id)
                        probed = True
                        got = snap.sale_code if id_kind == "code" else snap.sale_id
                        if got == wanted:
                              return snap
                        if id_kind == "internal" or snap.sale_id is None or snap.sale_code is None:
                              break
                        url_id = snap.sale_id + (wanted - snap.sale_code)
                        if url_id in seen or url_id <= 0:
                              break
            except NotInEditModeError:
                  if probed:  # an earlier probe loaded some other sale into the cart: clear it
                        try:
                              await self.discard()
                        except Exception:  # pragma: no cover
                              pass
                        raise
                  await self.discard()  # never leave a wrong sale loaded in the cart
                  raise SaleMismatchError(f"could not open sale {wanted} ({id_kind}); POS kept showing another sale")

      async def set_comment(self, text: str) -> SaleSnapshot:
            # Same payload the POS page sends on "Complete Sale" (finishSale(): comment + ref_sale_desc).
            resp = await self._c.session.post(
                  SET_COMMENT_PATH, data={"comment": text, "ref_sale_desc": ""}, headers=AJAX_HEADERS,
                  follow_redirects=True,
            )
            resp.raise_for_status()
            reload_resp = await self._c.session.get(RELOAD_PATH, follow_redirects=True)
            reload_resp.raise_for_status()
            return _safe_parse(reload_resp.text)

      async def complete(self) -> str:
            resp = await self._c.session.post(
                  COMPLETE_SALE_PATH, data={}, headers=AJAX_HEADERS, follow_redirects=True
            )
            if resp.status_code >= 500:
                  resp.raise_for_status()
            self.log.info("comment.completed", final_url=str(resp.url), status=resp.status_code)
            return str(resp.url)

      async def discard(self) -> None:
            resp = await self._c.session.post(
                  CANCEL_SALE_PATH, data={}, headers=AJAX_HEADERS, follow_redirects=True
            )
            resp.raise_for_status()

      # -- orchestration ----------------------------------------------------- #
      async def rewrite(
            self,
            sale_code: int,
            new_comment: str,
            *,
            expected_old_comment: str | None = None,
            expected_total=None,
            id_kind: str = "code",
            commit: bool = False,
            verify: bool = True,
      ) -> dict:
            async with self._lock:
                  before = await self.open_sale(sale_code, id_kind)
                  try:
                        if expected_total is not None and Decimal(str(expected_total)) != before.total:
                              raise SaleMismatchError(f"POS total {before.total} != expected {expected_total}")
                        if expected_old_comment is not None and norm(before.comment) != norm(expected_old_comment):
                              if norm(before.comment) == norm(new_comment):
                                    await self.discard()
                                    return {"sale": sale_code, "changed": False, "committed": False, "note": "already_done"}
                              raise SaleMismatchError(
                                    f"POS comment {before.comment!r} != expected {expected_old_comment!r}"
                              )
                        if before.comment == new_comment:
                              await self.discard()
                              return {"sale": sale_code, "changed": False, "committed": False, "note": "already_done"}

                        after = await self.set_comment(new_comment)
                        if after.comment != new_comment:
                              raise SaleMismatchError(f"comment not applied: cart shows {after.comment!r}")
                        if _content_fp(after) != _content_fp(before):
                              raise SaleMismatchError("cart contents changed while setting comment; aborting")

                        if not commit:
                              await self.discard()
                              return {"sale": sale_code, "changed": True, "committed": False,
                                    "old": before.comment, "new": new_comment}

                        final_url = await self.complete()
                  except Exception:
                        try:
                              await self.discard()
                        except Exception as exc:  # pragma: no cover
                              self.log.error("comment.discard_failed", error=str(exc))
                        raise

            if verify:
                  try:
                        check = await self.open_sale(sale_code, id_kind)
                  except Exception as exc:
                        raise CommitUnverifiedError(f"sale {sale_code}: re-open failed after commit: {exc!r}") from exc
                  try:
                        ok = check.comment == new_comment and _content_fp(check) == _content_fp(before)
                  finally:
                        await self.discard()
                  if not ok:
                        raise CommitUnverifiedError(
                              f"sale {sale_code}: after commit comment={check.comment!r}, contents_equal="
                              f"{_content_fp(check) == _content_fp(before)}"
                        )
                  return {"sale": sale_code, "changed": True, "committed": True, "verified": verify,
                        "old": before.comment, "new": new_comment, "final_url": final_url}