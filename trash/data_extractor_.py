from bs4 import BeautifulSoup

class DataExtractor:
      def __init__(self):
            self.accounts = ['MONIEPOINT', 'CASH', 'ACCESS BANK', 'STORE ACCOUNT']
            
      def getAccountID(self, account_name):
            account_mapping = {
                  'CASH': 3,
                  'MONIEPOINT': 2,
                  'ACCESS BANK': 1,
                  'STORE ACCOUNT': 4
            }
            return account_mapping.get(account_name.upper(), None)

      def extract_sales_item_data(self, soup: BeautifulSoup):
            items = []
            for tbody in soup.select('#receipt-draggable tbody[data-item-class="item"]'):
                  items.append({
                        "sale_id": int(tbody['data-sale-id']),
                        "item_id": int(tbody['data-item-id']),
                        "name": tbody['data-item-name'],
                        "qty": float(tbody['data-item-qty']),
                        "unit_price": int(tbody['data-item-price']),
                        "total": int(tbody['data-item-total'])
                  })
                  
            return items
      
      def parse_amount(self, amount_str):
            try:
                  return float(amount_str.replace(',', '').replace('₦', '').strip())
            except ValueError:
                  return 0
      
      def invoice_payment_to_dict(self, invoice_accounts, invoice_payments):
            invoice_payment_info = []
            for incts in range(len(invoice_accounts)):
                  p_info = {
                        'account': invoice_accounts[incts],
                        'amount': self.parse_amount(invoice_payments[incts]) if incts < len(invoice_payments) else 0.0,
                        'transaction_medium': 'Cash Payment' if invoice_accounts[incts] == 'CASH' else 'Bank Transfer',
                        'account_id': self.getAccountID(invoice_accounts[incts])
                  }
                  invoice_payment_info.append(p_info)
                  
            return invoice_payment_info
      
      def extract_invoice_data(self, soup: BeautifulSoup):
            
            # Extract the invoice account by checking for known account names in the invoice footer values
            invoice_accounts = []
            invoice_payments = []
            account_elements = soup.select(f'.invoice-footer .invoice-footer-value')
            invoice_payment_elements = soup.select(f'.invoice-footer .invoice-footer-value.invoice-payment')
            
            invoice_total = soup.select_one('.invoice-footer .invoice-footer-value.invoice-total').text.strip()
            invoice_policy = soup.select_one('.invoice-policy').text.strip()
            invoice_datetime = soup.select('.invoice-footer-heading')[3].text.strip()
            invoice_to = soup.select_one("ul.invoice-address.invoiceto li:nth-of-type(2)").get_text(strip=True)
            
            tt_count_element = soup.select('.invoice-footer .invoice-footer-value.invoice-total')
            
            total_items_count = round(float(tt_count_element[1].text.strip())) if tt_count_element else 0
            
            if not account_elements and not invoice_payment_elements:
                  return {}
            
            for account_element in account_elements:
                  account_text = account_element.text.strip()
                  if account_text and account_text.upper() in self.accounts:
                        invoice_accounts.append(account_text.upper())
                        
            for payment_element in invoice_payment_elements:
                  payment_text = payment_element.text.strip()
                  if payment_text:
                        invoice_payments.append(payment_text)
                                    
            invoice_payment_info = self.invoice_payment_to_dict(invoice_accounts, invoice_payments)
            
            invoice_data_dict = {
                  "invoice_total": self.parse_amount(invoice_total),
                  "invoice_policy": invoice_policy,
                  "invoice_payments": invoice_payment_info,
                  "total_items_count": total_items_count,
                  "invoice_to": str(invoice_to).upper().strip().replace('CUSTOMER', '').replace(':', '') if invoice_to else "UNKNOWN CUSTOMER",
                  "invoice_datetime": invoice_datetime
            }
                  
            return invoice_data_dict
      
      
      
      
      
      
      
      
      
from bs4 import BeautifulSoup
from dataclasses import dataclass
from pos4africa.shared.models.sale import RawSaleItem, RawPayment, PaymentChannel
from pos4africa.shared.errors.scraper import ScraperError, ElementNotFoundError, EmptyValueError
from pos4africa.shared.errors.codes import ErrorCodes

class Scraper:
      
      # Known anonymous/generic customer account names
      _ANONYMOUS_ACCOUNTS = ["indoor", "online", "online customers"]
      _PAYMENT_CHANNELS = ["MONIEPOINT", "ACCESS", "CASH", "STORE ACCOUNT", "TRADE BY BARTER", "STANBIC IBTC"]
      
      def __init__(self, sale_id: int, html_content: str):
                       
            if not html_content or not sale_id:
                  raise ValueError("HTML content or sale id cannot be empty.")
            
            self.soup = BeautifulSoup(html_content, "html.parser")
            self.sale_id = sale_id
      
      def check_element_exists(self, element, msg: str) -> None:
            """Checks the HTML element was found in the soup."""
            if element is None:
                  raise ElementNotFoundError(
                        message=f"[Invoice {self.sale_id}] {msg}",
                        code=ErrorCodes.POSSIBLE_HTML_STRUCTURE_CHANGED,
                        sale_id=self.sale_id
                  )

      def check_element_value(self, value: str, msg: str) -> None:
            """Checks the extracted text value is not empty."""
            if not value or not value.strip():
                  raise EmptyValueError(
                        message=f"[Invoice {self.sale_id}] {msg}",
                        code=ErrorCodes.EMPTY_VALUE,
                        sale_id=self.sale_id
                  )
      
      def get_raw_sale_items(self) -> list[RawSaleItem]:
            items = []
            tbodys = self.soup.select('#receipt-draggable tbody[data-item-class="item"]')
            
            self.check_element_exists(tbodys, "No sale items found in receipt.")

            for tbody in tbodys:
                  item = RawSaleItem(
                        pos_sale_id=tbody.get('data-sale-id'),
                        pos_prd_id=tbody.get('data-item-id'),
                        name=tbody.get('data-item-name'),
                        quantity=tbody.get('data-item-qty'),
                        unit_price=tbody.get('data-item-price'),
                        total=tbody.get('data-item-total')
                  )
                  items.append(item)
                  
            return items
      
      def get_customer(self) -> str:
            element = self.soup.select_one("ul.invoice-address.invoiceto li:nth-of-type(2)")
            self.check_element_exists(element, "Customer/Invoice to element not found. Possible HTML structure change.")
            
            customer = element.get_text(strip=True)
            self.check_element_value(customer, "Customer is empty, Data extraction failed.")

            return customer.replace('Customer:', '').strip()
      
      def get_salesperson(self) -> str | None:
            element = self.soup.select_one('ul.invoice-detail li:nth-child(4)')

            self.check_element_exists(element, 'Salesperson element not found.')

            span = element.find('span')
            if span:
                  span.extract()  # remove "Employee:"

            salesperson = element.get_text(strip=True)

            self.check_element_value(salesperson, 'Salesperson is empty.')

            return salesperson
      
      def get_comment(self) -> str:
            element = self.soup.select_one('.invoice-policy')
            
            self.check_element_exists(element, 'Comment element not found.')
            
            comment = element.get_text(strip=True)
            
            # self.check_element_value(comment, 'Comment is empty.')
            
            return comment.strip()
      
      def is_anonymous_customer(self) -> bool:
            name = self.get_customer() or ""
            return any(kw in name.lower() for kw in self._ANONYMOUS_ACCOUNTS)
      
      def get_invoice_total(self) -> str:
            element = self.soup.select_one('.invoice-footer .invoice-footer-value.invoice-total')
            self.check_element_exists(element, "Invoice total element not found. Possible HTML structure change.")

            invoice_total = element.get_text(strip=True)
            self.check_element_value(invoice_total, "Invoice total is empty. Data extraction failed.")

            return invoice_total
      
      def invoice_payment_to_dict(self, payment_channel, invoice_payments) -> list[RawPayment]:
            invoice_payment_info: list[RawPayment] = []
            
            if not payment_channel or not invoice_payments:
                  raise ScraperError(
                        message=f"[Invoice {self.sale_id}] Payment channel and invoice payments are empty.",
                        code=ErrorCodes.EMPTY_VALUE,
                        sale_id=self.sale_id
                  )
            for i in range(len(payment_channel)):
                  invoice_payment_info.append(RawPayment(
                        account=payment_channel[i],
                        amount=invoice_payments[i] if i < len(invoice_payments) else "0.0"
                  ))
            return invoice_payment_info
      
      def get_invoice_payments(self) -> list[RawPayment]:
            
            account_elements = self.soup.select(f'.invoice-footer .invoice-footer-value')
            invoice_payment_elements = self.soup.select(f'.invoice-footer .invoice-footer-value.invoice-payment')
            
            payments: list[RawPayment] = []
            payment_channels = []
            invoice_payments = []
            
            self.check_element_exists(account_elements, 'Rows element for extracting payments not found. Possible HTML structure change.')
            self.check_element_exists(invoice_payment_elements, 'Rows element for extracting payments not found. Possible HTML structure change.')
            
            for el in account_elements:
                  p_channel = el.get_text(strip=True)
                  if p_channel and p_channel.upper() in self._PAYMENT_CHANNELS:
                        payment_channels.append(p_channel.upper())
            
            for el in invoice_payment_elements:
                  payment = el.get_text(strip=True)
                  if payment: invoice_payments.append(payment)
            
            self.check_element_exists(payment_channels, 'No recognised payment channels found.')

            return self.invoice_payment_to_dict(payment_channels, invoice_payments)
      
      def get_change_due(self) -> str | None:
            elements = self.soup.select('.invoice-footer-value.invoice-total')
            self.check_element_exists(elements, "Change Due invoice-total element not found.")
            
            change_due_el = elements[3]
            
            change_due = change_due_el.get_text(strip=True)
            self.check_element_value(change_due, 'Change Due is empty.')
            
            return change_due
      
      def get_items_sold(self) -> str | None:
            elements = self.soup.select('.invoice-footer-value.invoice-total')
            self.check_element_exists(elements, "Items sold element not found. - 1")
            
            items_sold_el = elements[1]
            self.check_element_exists(items_sold_el, "Items sold element not found. - 2")
            
            items_sold = items_sold_el.get_text(strip=True)
            
            self.check_element_value(items_sold, "Items sold is empty.")

            return items_sold

      def get_invoice_datetime(self) -> str | None:
            invoice_datetime_els = self.soup.select('.invoice-footer-heading')
            
            self.check_element_exists(invoice_datetime_els, "Invoice Datetime element not found.")
            
            invoice_datetime_el = invoice_datetime_els[3]
            self.check_element_exists(invoice_datetime_el, "Invoice Datetime element not found.")
            
            invoice_datetime = invoice_datetime_el.get_text(strip=True)
            
            self.check_element_value(invoice_datetime, 'Invoice Datetime is empty.')
            
            return invoice_datetime
            
            
            



