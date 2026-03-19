from bs4 import BeautifulSoup
from pos4africa.shared.models.sale import RawSale, RawSaleItem, RawPayment
from pos4africa.shared.exceptions.scraper import ScraperError, ElementNotFoundError, EmptyValueError
from pos4africa.shared.exceptions.codes import ErrorCodes


class Scraper:

      _ANONYMOUS_ACCOUNTS = ["indoor", "online", "online customers"]
      _PAYMENT_CHANNELS   = [
            "MONIEPOINT", "ACCESS", "CASH",
            "STORE ACCOUNT", "TRADE BY BARTER", "STANBIC IBTC"
      ]

      def __init__(self, sale_id: str, html_content: str):
            if not sale_id or not html_content:
                  raise ScraperError(
                  message="sale_id and html_content cannot be empty.",
                  code=ErrorCodes.EMPTY_VALUE,
                  sale_id=sale_id or "UNKNOWN"
                  )
            self.sale_id = sale_id
            self.soup    = BeautifulSoup(html_content, "html.parser")

      # ── Main entry point ───────────────────────────────────────────────────────
      
      def run(self) -> RawSale:
            return self._scrape()

      def _scrape(self) -> RawSale:
            """
            Single method the pipeline calls.
            Returns RawSale — all fields str | None, no casting here.
            """
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

      # ── Guard helpers ──────────────────────────────────────────────────────────

      def _require_element(self, element, msg: str) -> None:
            """Raises ElementNotFoundError if element is None or empty list."""
            if element is None or element == []:
                  raise ElementNotFoundError(
                        message=f"[Invoice {self.sale_id}] {msg}",
                        code=ErrorCodes.POSSIBLE_HTML_STRUCTURE_CHANGED,
                        sale_id=self.sale_id
                  )

      def _require_value(self, value: str | None, msg: str) -> None:
            """Raises EmptyValueError if value is None or blank string."""
            if not value or not value.strip():
                  raise EmptyValueError(
                        message=f"[Invoice {self.sale_id}] {msg}",
                        code=ErrorCodes.EMPTY_VALUE,
                        sale_id=self.sale_id
                  )

      def _extract_optional(self, selector: str) -> str | None:
            """Returns text for optional fields — None if missing or empty."""
            element = self.soup.select_one(selector)
            if element is None:
                  return None
            value = element.get_text(strip=True)
            return value if value else None

      # ── Required fields ────────────────────────────────────────────────────────
      def _extract_customer(self) -> str:
            element = self.soup.select_one("ul.invoice-address.invoiceto li:nth-of-type(2)")
            self._require_element(element, "Customer element not found.")

            customer = element.get_text(strip=True)
            self._require_value(customer, "Customer value is empty.")

            return customer.replace("Customer:", "").strip()

      def _is_anonymous_customer(self) -> bool:
            # Reuses already-extracted customer name — no double parse
            try:
                  name = self._extract_customer()
            except (ElementNotFoundError, EmptyValueError):
                  return False
            return any(kw in name.lower() for kw in self._ANONYMOUS_ACCOUNTS)

      def _extract_invoice_total(self) -> str:
            element = self.soup.select_one(".invoice-footer .invoice-footer-value.invoice-total")
            self._require_element(element, "Invoice total element not found.")

            total = element.get_text(strip=True)
            self._require_value(total, "Invoice total is empty.")

            return total

      def _extract_salesperson(self) -> str:
            element = self.soup.select_one("ul.invoice-detail li:nth-child(4)")
            self._require_element(element, "Salesperson element not found.")

            # Remove the "Employee:" label span before extracting text
            label = element.find("span")
            if label:
                  label.extract()

            salesperson = element.get_text(strip=True)
            self._require_value(salesperson, "Salesperson value is empty.")

            return salesperson

      def _extract_invoice_datetime(self) -> str:
            # The datetime is in the invoice-detail block inside a <strong> tag
            element = self.soup.select_one("ul.invoice-detail strong")
            self._require_element(element, "Invoice datetime element not found.")

            dt = element.get_text(strip=True)
            self._require_value(dt, "Invoice datetime is empty.")

            return dt

      def _extract_sale_items(self) -> list[RawSaleItem]:
            tbodys = self.soup.select(
                  '#receipt-draggable tbody[data-item-class="item"]'
            )
            self._require_element(tbodys, "No sale items found in receipt.")

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
            all_footer_values = self.soup.select(".invoice-footer .invoice-footer-value")
            payment_amount_els = self.soup.select(".invoice-footer .invoice-footer-value.invoice-payment")

            self._require_element(all_footer_values,  "Footer value elements not found.")
            self._require_element(payment_amount_els, "Payment amount elements not found.")

            # Extract recognised channels from all footer values
            channels = []
            for el in all_footer_values:
                  text = el.get_text(strip=True).upper()
                  if text in self._PAYMENT_CHANNELS:
                        channels.append(text)

            self._require_element(channels, "No recognised payment channels found.")

            # Extract payment amounts
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

      # ── Optional fields ────────────────────────────────────────────────────────
      def _extract_comment(self) -> str | None:
            return self._extract_optional(".invoice-policy")

      def _extract_change_due(self) -> str | None:
            # Specific selector — not index-based
            element = self.soup.select(".invoice-footer-value.invoice-total")[3]
            return element.get_text(strip=True) if element else None

      def _extract_items_sold(self) -> str | None:
            element = self.soup.select(".invoice-footer-value.invoice-total")[1]
            return element.get_text(strip=True) if element else None

      def _extract_items_returned(self) -> str | None:
            element = self.soup.select(".invoice-footer-value.invoice-total")[1]
            return element.get_text(strip=True) if element else None