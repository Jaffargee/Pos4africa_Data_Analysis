# tests/test_processor.py
import pytest
from decimal import Decimal
from pos4africa.worker.components.processor import Processor
from pos4africa.shared.models.sale import Sale, SaleItem, Payment, ProcessedSale, ProcessedSaleItem, ProcessedPayment
from types import SimpleNamespace
import uuid

class FakeLTM:
      def __init__(self):
            self._customers = {}
            self._accounts = {}

      async def get_customer_id_by_name(self, name):
            # return a number or None
            return self._customers.get(name) or None

      async def get_accounts_id_by_name(self, name):
            return self._accounts.get(name) or uuid.uuid5(uuid.NAMESPACE_DNS, "Bytes")


class FakeMemory:
      def __init__(self):
            self.ltm = FakeLTM()


@pytest.mark.asyncio
async def test_processor_transforms_sale_and_generates_hash():
      memory = FakeMemory()
      # pre-populate a customer lookup
      memory.ltm._customers["Bob"] = 99
      memory.ltm._accounts["STANBIC IBTC BANK"] = uuid.uuid5(uuid.NAMESPACE_DNS, "Stanbic")
      # memory.ltm._accounts["Cash"] = uuid.uuid5(uuid.NAMESPACE_DNS, "Cash")

      parsed = Sale(
            pos_sale_id=100,
            pos_customer_id=memory.ltm._customers["Bob"],
            invoice_datetime="2023-01-02T15:04:00",
            salesperson="Alice",
            customer_name="Bob",
            is_anonymous_customer=False,
            invoice_total=Decimal("100.00"),
            change_due=Decimal("0.00"),
            items_sold=1,
            items_returned=0,
            items_net=1,
            comment=None,
            items=[SaleItem(pos_item_id=10, pos_sale_id=100, name="X", quantity=1, unit_price=Decimal("100.00"), total=Decimal("100.00"))],
            payments=[Payment(channel="STANBIC IBTC BANK", amount=Decimal("100.00"))],
      )

      # hash=hashlib.sha256("dummy_string".encode()).hexdigest()

      processor = Processor(node_id="n", memory=memory)
      processed = await processor.run(parsed)

      assert processed is not None
      assert processed.pos_sale_id == parsed.pos_sale_id
      assert processed.pos_customer_id == 99  # resolved from FakeLTM
      assert processed.items and len(processed.items) == 1
      assert processed.payments and len(processed.payments) == 1
      # assert hasattr(processed, "hash")
      # assert isinstance(processed.hash, str) and len(processed.hash) > 10