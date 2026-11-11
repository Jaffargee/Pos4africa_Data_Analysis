from __future__ import annotations

from uuid import UUID

import pytest

from server.config import ServerSettings
from server.container import assemble
from server.tests.fakes import (
    FakeCustomerGateway,
    FakeCustomerRepo,
    FakeItemRepo,
    FakeSaleRepo,
    FakeSalesGateway,
    InMemoryWriteLog,
)

API_KEY = "k" * 24
CASH = UUID("11111111-1111-1111-1111-111111111111")


def make_settings(mode: str = "commit") -> ServerSettings:
    return ServerSettings(server_api_key=API_KEY, pos_write_mode=mode, _env_file=None)


class World:
    """One wired container plus handles on every fake."""

    def __init__(self, mode: str = "commit"):
        self.log = InMemoryWriteLog()
        self.customers = FakeCustomerRepo()
        self.pos_customers = FakeCustomerGateway()
        self.sales = FakeSaleRepo()
        self.pos_sales = FakeSalesGateway()
        self.items = FakeItemRepo()
        self.sales.accounts[CASH] = "CASH"
        self.items.add(1, "Rice 50kg", 80000, cost_price=70000)
        self.items.add(2, "Beans 25kg", 45000, cost_price=40000)
        self.container = assemble(
            make_settings(mode), write_log=self.log, customers=self.customers, sales=self.sales, items=self.items,
            pos_customers=self.pos_customers, pos_sales=self.pos_sales,
        )


@pytest.fixture
def world() -> World:
    return World()


@pytest.fixture
def dry_world() -> World:
    return World(mode="dry_run")
