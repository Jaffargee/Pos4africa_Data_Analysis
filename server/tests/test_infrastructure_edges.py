"""Pure/edge pieces of the infrastructure layer: no network, no credentials."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from pos4africa.shared.utils.sale_ref import attach_ref, extract_ref, ref_from_key, strip_ref
from server.domain.errors import (
    PosOutcomeUnknownError,
    PosRejectedError,
    PosSessionExpiredError,
    PosUnavailableError,
)
from server.domain.sales.models import ResolvedLine, ResolvedPayment, ResolvedSale, compute_fingerprint
from server.infrastructure.pos4africa.errors import classify_http_error, classify_pre_commit_error
from server.infrastructure.pos4africa.html import parse_cart, parse_form_fields, verify_cart
from server.infrastructure.pos4africa.session_pool import PosSessionPool
from server.infrastructure.supabase._client import safe_term


# -- ref helpers ----------------------------------------------------------------------
def test_ref_roundtrip_and_strip():
    ref = ref_from_key("SALE_CREATE", "abc-123")
    assert ref == ref_from_key("SALE_CREATE", "abc-123") and len(ref) == 10
    assert ref != ref_from_key("CUSTOMER_CREATE", "abc-123")

    tagged = attach_ref("ctm fatima", ref)
    assert extract_ref(tagged) == ref and strip_ref(tagged) == "ctm fatima"
    assert strip_ref(attach_ref(None, ref)) is None          # ref-only comment disappears
    assert strip_ref("plain comment") == "plain comment"
    assert extract_ref(tagged.upper()) == ref                 # the ETL upper-cases comments
    assert extract_ref(None) is None and strip_ref(None) is None


# -- fingerprint ----------------------------------------------------------------------
def test_fingerprint_ignores_line_order_and_decimal_spelling_but_not_content():
    a = compute_fingerprint(10, [(1, Decimal("2"), Decimal("80000")), (2, Decimal("1"), Decimal("44000.00"))], Decimal("204000"), "R")
    b = compute_fingerprint(10, [(2, Decimal("1.0"), Decimal("44000")), (1, Decimal("2.00"), Decimal("80000.0"))], Decimal("204000.00"), "R")
    assert a == b
    assert a != compute_fingerprint(10, [(1, Decimal("3"), Decimal("80000"))], Decimal("204000"), "R")
    assert a != compute_fingerprint(11, [(1, Decimal("2"), Decimal("80000")), (2, Decimal("1"), Decimal("44000"))], Decimal("204000"), "R")
    assert a != compute_fingerprint(10, [(1, Decimal("2"), Decimal("80000")), (2, Decimal("1"), Decimal("44000"))], Decimal("204000"), "S")


# -- error classification ----------------------------------------------------------------
def _status_error(code: int) -> httpx.HTTPStatusError:
    req = httpx.Request("POST", "https://pos.example/x")
    return httpx.HTTPStatusError("x", request=req, response=httpx.Response(code, request=req))


@pytest.mark.parametrize("exc, expected", [
    (httpx.ConnectError("refused"), PosUnavailableError),
    (httpx.ConnectTimeout("slow"), PosUnavailableError),
    (httpx.ReadTimeout("no reply"), PosOutcomeUnknownError),     # request was sent: might have applied
    (httpx.RemoteProtocolError("dropped"), PosOutcomeUnknownError),
    (_status_error(404), PosRejectedError),
    (_status_error(502), PosOutcomeUnknownError),
])
def test_write_failure_classification(exc, expected):
    assert type(classify_http_error(exc)) is expected


def test_pre_commit_failures_are_always_safe_to_retry():
    assert isinstance(classify_pre_commit_error(httpx.ReadTimeout("x")), PosUnavailableError)
    assert isinstance(classify_pre_commit_error(_status_error(500)), PosUnavailableError)
    assert isinstance(classify_pre_commit_error(_status_error(422)), PosRejectedError)
    already = PosRejectedError("cart mismatch")
    assert classify_pre_commit_error(already) is already


# -- HTML parsing -------------------------------------------------------------------------
CART_HTML = """
<table>
 <tbody class="register-item-content"><tr><td>
   <a class="register-item-name">Rice 50kg</a>
   <a data-name="unit_price" data-value="80000.00">80,000.00</a>
   <a data-name="quantity" data-value="2">2</a>
   <a data-name="total" data-value="160000.00">160,000.00</a></td></tr></tbody>
</table>
<div class="amount-block"><div class="total amount"><span class="total-amount">₦160,000.00</span></div></div>
<ul class="payments"><li class="list-group-item"><span class="key">CASH</span><span class="value">₦160,000.00</span></li></ul>
<textarea id="comment">deliver ~R:AB12CD34EF</textarea>
"""


def _sale(**over) -> ResolvedSale:
    base = dict(
        ref="AB12CD34EF", pos_customer_id=10, customer_name="Aisha", salesperson="x", comment="deliver",
        comment_with_ref="deliver ~R:AB12CD34EF", total=Decimal("160000.00"), fingerprint="f",
        lines=[ResolvedLine(pos_item_id=1, name="Rice", quantity=Decimal("2"), unit_price=Decimal("80000.00"), total=Decimal("160000.00"))],
        payments=[ResolvedPayment(account_id=uuid4(), account="CASH", amount=Decimal("160000.00"))],
    )
    return ResolvedSale(**{**base, **over})


def test_parse_cart_and_verify():
    cart = parse_cart(CART_HTML)
    assert cart.total == Decimal("160000.00") and cart.payments_total == Decimal("160000.00")
    assert cart.lines[0].quantity == Decimal("2") and "AB12CD34EF" in cart.comment
    verify_cart(cart, _sale())  # exact match: no error


@pytest.mark.parametrize("over, message", [
    ({"total": Decimal("1")}, "total"),
    ({"ref": "0000000000", "comment_with_ref": "x"}, "ref"),
    ({"lines": [ResolvedLine(pos_item_id=1, name="R", quantity=Decimal("3"), unit_price=Decimal("80000.00"), total=Decimal("1"))]}, "line 1"),
])
def test_verify_cart_refuses_anything_but_the_exact_sale(over, message):
    with pytest.raises(PosRejectedError, match=message):
        verify_cart(parse_cart(CART_HTML), _sale(**over))


FORM_HTML = """
<form id="customer_form">
  <input name="first_name" value="Aisha"><input name="last_name" value="Bello">
  <input type="checkbox" name="taxable" value="1" checked><input type="checkbox" name="other" value="1">
  <textarea name="comments">vip</textarea>
  <select name="state"><option value="">-</option><option value="Kano" selected>Kano</option></select>
  <input type="submit" name="go" value="Save">
</form>
<form id="search"><input name="q" value="zzz"></form>
"""


def test_parse_form_fields_reads_the_customer_form_only():
    fields = parse_form_fields(FORM_HTML)
    assert fields == {"first_name": "Aisha", "last_name": "Bello", "taxable": "1", "comments": "vip", "state": "Kano"}
    assert parse_form_fields("<html>login page</html>") == {}  # unreadable -> caller must refuse to write


def test_safe_term_strips_postgrest_filter_syntax():
    assert safe_term("a),b.eq.(1") == "a b.eq. 1"
    assert safe_term("  Musa  Ibrahim ") == "Musa Ibrahim"
    assert safe_term("%*\\") is None and safe_term(None) is None


# -- session pool ---------------------------------------------------------------------------
class FakeConn:
    n = 0

    def __init__(self):
        FakeConn.n += 1
        self.id = FakeConn.n
        self.closed = False

    async def init(self):
        return self

    async def close(self):
        self.closed = True


async def test_pool_serialises_use_of_one_session():
    pool = PosSessionPool(1, factory=FakeConn)
    await pool.start()
    active, peak = 0, 0

    async def use():
        nonlocal active, peak
        async with pool.acquire():
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.01)
            active -= 1

    await asyncio.gather(*(use() for _ in range(5)))
    assert peak == 1  # the POS cart lives in the session: never two flows at once
    await pool.stop()


async def test_pool_replaces_an_expired_session_and_times_out_when_busy():
    pool = PosSessionPool(1, acquire_timeout=0.05, factory=FakeConn)
    await pool.start()
    async with pool.acquire() as first:
        with pytest.raises(PosUnavailableError, match="busy"):
            async with pool.acquire():
                pass
    with pytest.raises(PosSessionExpiredError):
        async with pool.acquire() as conn:
            raise PosSessionExpiredError("logged out")
    async with pool.acquire() as fresh:
        assert fresh is not conn and conn.closed
    await pool.stop()
