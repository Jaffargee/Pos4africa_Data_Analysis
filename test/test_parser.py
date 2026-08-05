# tests/test_parser.py
import pytest
from decimal import Decimal
from datetime import datetime

from pos4africa.worker.components.parser import Parser
from pos4africa.shared.models.sale import RawSale, RawSaleItem, RawPayment


@pytest.mark.asyncio
async def test_parser_parses_valid_raw_sale():
      raw = RawSale(
            pos_sale_id="123",
            invoice_datetime="01/02/2023 03:04 PM",
            salesperson=" Alice  Smith ",
            customer_name=" Bob ",
            is_anonymous_customer=False,
            invoice_total="₦1,234.50",
            change_due="₦0.50",
            items_sold="2",
            items_returned="0",
            comment="  test  ",
            items=[
                  RawSaleItem(
                  pos_sale_id="123",
                  pos_item_id="10",
                  name="Widget A",
                  quantity="1",
                  unit_price="₦500.00",
                  total="₦500.00",
                  ),
                  RawSaleItem(
                  pos_sale_id="123",
                  pos_item_id="11",
                  name="Widget B",
                  quantity="1",
                  unit_price="₦734.50",
                  total="₦734.50",
                  ),
            ],
            payments=[RawPayment(channel="Cash", amount="₦1,234.50")],
      )

      parser = Parser(node_id="n", memory=None)
      parsed = await parser.run(raw)

      assert parsed.pos_sale_id == 123
      assert isinstance(parsed.invoice_datetime, datetime)
      assert parsed.salesperson == "Alice Smith"
      assert parsed.customer_name == "Bob"
      assert parsed.invoice_total == Decimal("1234.50")
      assert parsed.items_sold == 2
      assert parsed.items_net == 2
      assert len(parsed.items) == 2
      assert len(parsed.payments) == 1
      assert parsed.payments[0].amount == Decimal("1234.50")


@pytest.mark.asyncio
async def test_parser_missing_required_field_raises():
      raw = RawSale(
            pos_sale_id=None,
            invoice_datetime="2023-01-02T15:04:00",
            salesperson="S",
            customer_name="C",
            invoice_total="₦10.00",
            items_sold="1",
      )
      parser = Parser(node_id="n", memory=None)
      with pytest.raises(ValueError):
            await parser.run(raw)


@pytest.mark.asyncio
async def test_parser_handles_alternate_date_formats():
      raw = RawSale(
            pos_sale_id="1",
            invoice_datetime="2023-01-02T15:04:00",
            salesperson="S",
            customer_name="C",
            invoice_total="₦10.00",
            items_sold="1",
      )
      parser = Parser(node_id="n", memory=None)
      parsed = await parser.run(raw)
      assert parsed.invoice_datetime.year == 2023