"""Pure HTML parsing for POS pages.  No settings, no network: unit-testable."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal

from bs4 import BeautifulSoup

from server.domain.errors import PosRejectedError
from server.domain.sales.models import ResolvedSale


def money_from(text: str | None) -> Decimal:
    cleaned = re.sub(r"[^\d.\-]", "", text or "")
    return Decimal(cleaned) if cleaned not in ("", "-", ".") else Decimal("0")


def _val(tag) -> str:
    if tag is None:
        return ""
    return tag.get("data-value") or tag.get_text(strip=True)


@dataclass(frozen=True)
class CartLine:
    name: str
    unit_price: Decimal
    quantity: Decimal
    total: Decimal


@dataclass(frozen=True)
class Cart:
    total: Decimal
    comment: str
    lines: tuple[CartLine, ...] = field(default_factory=tuple)
    payments_total: Decimal = Decimal("0")


def parse_cart(html: str) -> Cart:
    """Read the register page.  Selectors mirror the ones EditSale already uses on
    the real edit page, but this does not require the 'Editing Sale' banner."""
    soup = BeautifulSoup(html, "html.parser")
    lines = []
    for body in soup.select("tbody.register-item-content"):
        lines.append(CartLine(
            name=(lambda t: t.get_text(strip=True) if t else "")(body.select_one("a.register-item-name")),
            unit_price=money_from(_val(body.select_one('a[data-name="unit_price"]'))),
            quantity=money_from(_val(body.select_one('a[data-name="quantity"]'))),
            total=money_from(_val(body.select_one('a[data-name="total"]'))),
        ))
    paid = Decimal("0")
    for li in soup.select("ul.payments li.list-group-item"):
        value = li.select_one(".value")
        if value:
            paid += money_from(value.get_text())
    total_el = soup.select_one(".amount-block .total.amount .total-amount")
    comment_el = soup.select_one("textarea#comment")
    return Cart(
        total=money_from(total_el.get_text() if total_el else None),
        comment=comment_el.get_text(strip=True) if comment_el else "",
        lines=tuple(lines), payments_total=paid,
    )


def verify_cart(cart: Cart, sale: ResolvedSale) -> None:
    """The cart POS built must be exactly the sale we intend.  Raise before completing if not."""
    if len(cart.lines) != len(sale.lines):
        raise PosRejectedError(f"POS cart has {len(cart.lines)} lines, expected {len(sale.lines)}")
    for n, (got, want) in enumerate(zip(cart.lines, sale.lines, strict=True), start=1):
        if got.quantity != want.quantity or got.unit_price != want.unit_price:
            raise PosRejectedError(
                f"POS cart line {n} is {got.quantity} x {got.unit_price}, "
                f"expected {want.quantity} x {want.unit_price}"
            )
    if cart.total != sale.total:
        raise PosRejectedError(f"POS cart total {cart.total} != expected {sale.total}")
    if cart.payments_total != sale.total:
        raise PosRejectedError(f"POS cart payments {cart.payments_total} != expected {sale.total}")
    if sale.ref not in cart.comment:
        raise PosRejectedError("POS cart comment does not carry the sale ref")


def parse_form_fields(html: str) -> dict[str, str]:
    """Current values of the customer form (inputs, textareas, selects), keyed by field name."""
    soup = BeautifulSoup(html, "html.parser")
    form = next((f for f in soup.find_all("form") if f.find(attrs={"name": "first_name"})), None)
    if form is None:
        return {}
    out: dict[str, str] = {}
    for el in form.find_all(["input", "textarea", "select"]):
        name = el.get("name")
        if not name:
            continue
        if el.name == "textarea":
            out[name] = el.get_text()
        elif el.name == "select":
            chosen = el.find("option", selected=True) or el.find("option")
            out[name] = (chosen.get("value") or "") if chosen else ""
        else:
            kind = (el.get("type") or "text").lower()
            if kind in ("checkbox", "radio"):
                if el.has_attr("checked"):
                    out[name] = el.get("value", "1")
            elif kind not in ("submit", "button", "file"):
                out[name] = el.get("value", "")
    return out
