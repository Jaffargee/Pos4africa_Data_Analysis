import pytest
from pathlib import Path
from pos4africa.worker.components.scraper import Scraper
from pos4africa.shared.models.sale import SaleItem, RawPayment

# Load the fixture HTML once for all tests
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "receipt.html"


@pytest.fixture
def html_content() -> str:
      return FIXTURE_PATH.read_text(encoding="utf-8")

@pytest.fixture
def extractor(html_content: str) -> Scraper:
      return Scraper(html_content=html_content, sale_id=1)

def test_raw_get_sale_item(extractor: Scraper):
      items = extractor.get_raw_sale_items()
      assert isinstance(items, list)
      assert len(items) > 0
      
def test_raw_get_customer(extractor: Scraper):
      customer = extractor.get_customer()
      assert isinstance(customer, str)
      assert len(customer) > 0
      
def test_raw_get_invoice_total(extractor: Scraper):
      invoice_total = extractor.get_invoice_total()
      assert isinstance(invoice_total, str)
      assert len(invoice_total) > 0
      
def test_raw_get_payments(extractor: Scraper):
      payments = extractor.get_invoice_payments()
      assert isinstance(payments, list)
      assert len(payments) > 0
      
def test_raw_get_change_due(extractor: Scraper):
      change_due = extractor.get_change_due()
      assert isinstance(change_due, str)
      assert len(change_due) > 0
      
def test_raw_get_items_sold(extractor: Scraper):
      items_sold = extractor.get_items_sold()
      assert isinstance(items_sold, str)
      assert len(items_sold) > 0
      
def test_raw_get_invoice_datetime(extractor: Scraper):
      invoice_datetime = extractor.get_invoice_datetime()
      assert isinstance(invoice_datetime, str)
      assert len(invoice_datetime) > 0
      
def test_raw_get_salesperson(extractor: Scraper):
      salesperson = extractor.get_salesperson()
      assert isinstance(salesperson, str)
      assert len(salesperson) > 0