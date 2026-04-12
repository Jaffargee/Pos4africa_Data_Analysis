from bs4 import BeautifulSoup
from pos4africa.shared.models.sale import RawSale, RawSaleItem, RawPayment
from pos4africa.shared.exceptions.scraper import ScraperError, ElementNotFoundError
from pos4africa.shared.exceptions.codes import ErrorCodes
from pos4africa.worker.components.base import BaseComponent
from pos4africa.manager.memory.store import MemoryStore


class Scraper(BaseComponent):

      _ANONYMOUS_ACCOUNTS = ["indoor", "online", "online customers"]
      _PAYMENT_CHANNELS   = [
            "MONIEPOINT", "ACCESS BANK", "CASH",
            "STORE ACCOUNT", "TRADE BY BARTER", "STANBIC IBTC"
      ]
      
      @property
      def soup(self) -> BeautifulSoup:
            """Guarded access to soup, raises if None."""
            if self._soup is None:
                  raise ScraperError(
                        message=f"[Invoice {self.sale_id}] BeautifulSoup not initialized. Call run() first.",
                        code=ErrorCodes.POSSIBLE_HTML_STRUCTURE_CHANGED,
                        sale_id=self.sale_id or "UNKNOWN"
                  )
            return self._soup

      @soup.setter
      def soup(self, value: BeautifulSoup | None):
            self._soup = value

      def __init__(self, node_id: str, memory: MemoryStore):
            super().__init__(node_id, memory)
            self.sale_id: int | None  = None
            self._soup: BeautifulSoup | None = None

      # ── Main entry ────────────────────────────────────────────────────────────

      async def run(self, sale_id: int, html_content: str) -> RawSale:
            if not sale_id or not html_content:
                  raise ScraperError(
                        message="sale_id and html_content cannot be empty.",
                        code=ErrorCodes.EMPTY_VALUE,
                        sale_id=sale_id or "UNKNOWN"
                  )

            self._soup = BeautifulSoup(html_content, "html.parser")
            self.sale_id = sale_id
            return self._scrape()

      def _scrape(self) -> RawSale:
            return RawSale(
                  pos_sale_id          = str(self.sale_id),
                  invoice_datetime     = self._extract_invoice_datetime(),
                  salesperson          = self._extract_salesperson(),
                  customer_name        = self._extract_customer(),
                  is_anonymous_customer= self._is_anonymous_customer(),
                  invoice_total        = self._extract_invoice_total(),
                  items_sold           = self._extract_items_sold(),
                  items_returned       = self._extract_items_returned(),
                  change_due           = self._extract_change_due(),
                  comment              = self._extract_comment(),
                  items                = self._extract_sale_items(),
                  payments             = self._extract_payments(),
            )

      # ── Guard helpers (ONLY for structural failures) ──────────────────────────

      def _require_element(self, element, msg: str) -> None:
            if element is None or element == []:
                  raise ElementNotFoundError(
                        message=f"[Invoice {self.sale_id}] {msg}",
                        code=ErrorCodes.POSSIBLE_HTML_STRUCTURE_CHANGED,
                        sale_id=self.sale_id
                  )

      def _extract_optional(self, selector: str) -> str | None:
            element = self.soup.select_one(selector)
            if not element:
                  return None

            value = element.get_text(strip=True)
            return value if value else None

      # ── Required (STRUCTURE ONLY, NOT VALUE STRICTNESS) ───────────────────────

      def _extract_customer(self) -> str | None:
            element = self.soup.select_one("ul.invoice-address.invoiceto li:nth-of-type(2)")

            if not element:
                  return None

            customer = element.get_text(strip=True)
            return customer.replace("Customer:", "").strip() if customer else None

      def _is_anonymous_customer(self) -> bool:
            name = self._extract_customer()
            if not name:
                  return False

            return any(kw in name.lower() for kw in self._ANONYMOUS_ACCOUNTS)

      def _extract_invoice_total(self) -> str | None:
            element = self._soup.select_one(".invoice-footer .invoice-footer-value.invoice-total")

            if not element:
                  return None

            total = element.get_text(strip=True)
            return total if total else None

      def _extract_salesperson(self) -> str | None:
            element = self._soup.select_one("ul.invoice-detail li:nth-child(4)")

            if not element:
                  return None

            label = element.find("span")
            if label:
                  label.extract()
            
            value = element.get_text(strip=True)
            return value if value else None

      def _extract_invoice_datetime(self) -> str | None:
            element = self._soup.select_one("ul.invoice-detail strong")

            if not element:
                  return None

            value = element.get_text(strip=True)
            print(value, element)
            return value if value else None

      def _extract_sale_items(self) -> list[RawSaleItem]:
            tbodys = self._soup.select('#receipt-draggable tbody[data-item-class="item"]')

            if not tbodys:
                  return []

            items = []
            for tbody in tbodys:
                  items.append(RawSaleItem(
                        pos_sale_id = tbody.get("data-sale-id"),
                        pos_prd_id  = tbody.get("data-item-id"),
                        name        = tbody.get("data-item-name"),
                        quantity    = tbody.get("data-item-qty"),
                        unit_price  = tbody.get("data-item-price"),
                        total       = tbody.get("data-item-total"),
                  ))

            return items

      def _extract_payments(self) -> list[RawPayment]:
            all_footer_values = self._soup.select(".invoice-footer .invoice-footer-value")
            payment_amount_els = self._soup.select(".invoice-footer .invoice-footer-value.invoice-payment")

            if not all_footer_values or not payment_amount_els:
                  return []

            channels = []
            for el in all_footer_values:
                  text = el.get_text(strip=True).upper()
                  if text in self._PAYMENT_CHANNELS:
                        channels.append(text)

            amounts = [
                  el.get_text(strip=True)
                  for el in payment_amount_els
                  if el.get_text(strip=True)
            ]

            return self._build_payments(channels, amounts)

      def _build_payments(self, channels: list[str], amounts: list[str]) -> list[RawPayment]:
            return [
                  RawPayment(
                        channel = channels[i],
                        amount  = amounts[i] if i < len(amounts) else "0.0",
                  )
                  for i in range(len(channels))
            ]

      # ── Optional fields ───────────────────────────────────────────────────────

      def _extract_comment(self) -> str | None:
            return self._extract_optional(".invoice-policy")

      def _extract_change_due(self) -> str | None:
            rows = self._soup.select(".invoice-footer-heading")

            for heading in rows:
                  if "change due" in heading.get_text(strip=True).lower():
                        value = heading.find_next(class_="invoice-footer-value")
                        return value.get_text(strip=True) if value else None

            return None

      def _extract_items_sold(self) -> str | None:
            rows = self._soup.select(".invoice-footer-heading")

            for heading in rows:
                  if "number of items sold" in heading.get_text(strip=True).lower():
                        value = heading.find_next(class_="invoice-footer-value")
                        return value.get_text(strip=True) if value else None

            return None

      def _extract_items_returned(self) -> str | None:
            rows = self._soup.select(".invoice-footer-heading")

            for heading in rows:
                  if "item returned" in heading.get_text(strip=True).lower():
                        value = heading.find_next(class_="invoice-footer-value")
                        return value.get_text(strip=True) if value else "0"

            return "0"

# def _require_value(self, value: str | None, msg: str) -> None:
#       """Raises EmptyValueError if value is None or blank string."""
#       if not value or not value.strip():
#             raise EmptyValueError(
#                   message=f"[Invoice {self.sale_id}] {msg}",
#                   code=ErrorCodes.EMPTY_VALUE,
#                   sale_id=self.sale_id
#             )