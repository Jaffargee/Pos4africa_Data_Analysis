"""Composition root: the only module that knows every concrete class.

Everything else depends on ports (``domain/*/ports.py``), so tests build a
``Container`` from fakes and never touch POS or Supabase.
"""

from __future__ import annotations

from dataclasses import dataclass

from server.application.customers.create import CreateCustomer, CreateCustomerHandler
from server.application.customers.update import UpdateCustomer, UpdateCustomerHandler
from server.application.pos_write import PosWriteSaga
from server.application.recon.reconcile import Reconciler
from server.application.sales.create import CreateSale, SaleHandler
from server.config import ServerSettings
from server.domain.customers.ports import CustomerRepository, PosCustomerGateway
from server.domain.ports import WriteLogRepository
from server.domain.sales.ports import ItemRepository, PosSalesGateway, SaleRepository
from server.domain.write_log import WriteKind


@dataclass
class Container:
    settings: ServerSettings
    write_log: WriteLogRepository
    customers: CustomerRepository
    sales: SaleRepository
    items: ItemRepository
    saga: PosWriteSaga
    create_customer: CreateCustomer
    update_customer: UpdateCustomer
    create_sale: CreateSale
    reconciler: Reconciler
    pool: object | None = None

    async def start(self) -> None:
        if self.pool is not None:
            await self.pool.start()

    async def stop(self) -> None:
        if self.pool is not None:
            await self.pool.stop()


def assemble(
    settings: ServerSettings,
    *,
    write_log: WriteLogRepository,
    customers: CustomerRepository,
    sales: SaleRepository,
    items: ItemRepository,
    pos_customers: PosCustomerGateway,
    pos_sales: PosSalesGateway,
    pool: object | None = None,
) -> Container:
    handlers = {
        WriteKind.CUSTOMER_CREATE: CreateCustomerHandler(pos_customers, customers),
        WriteKind.CUSTOMER_UPDATE: UpdateCustomerHandler(pos_customers, customers),
        WriteKind.SALE_CREATE: SaleHandler(pos_sales, sales),
    }
    saga = PosWriteSaga(write_log, handlers, settings.pos_write_mode)
    return Container(
        settings=settings, write_log=write_log, customers=customers, sales=sales, items=items, saga=saga,
        create_customer=CreateCustomer(saga, customers),
        update_customer=UpdateCustomer(saga, customers),
        create_sale=CreateSale(
            saga, sales, customers, items,
            walk_in_customer_id=settings.walk_in_customer_id, salesperson=settings.salesperson,
        ),
        reconciler=Reconciler(saga, write_log, settings.uncertain_grace_seconds),
        pool=pool,
    )


def build_container(settings: ServerSettings) -> Container:
    """Wire the real adapters (needs POS + Supabase credentials in the environment)."""
    from server.infrastructure.pos4africa.customers import PosCustomerGatewayImpl
    from server.infrastructure.pos4africa.sales import PosSalesGatewayImpl
    from server.infrastructure.pos4africa.session_pool import PosSessionPool
    from server.infrastructure.supabase.customers import SupabaseCustomerRepository
    from server.infrastructure.supabase.items import SupabaseItemRepository
    from server.infrastructure.supabase.sales import SupabaseSaleRepository
    from server.infrastructure.supabase.write_log import SupabaseWriteLog

    pool = PosSessionPool(settings.pos_session_pool_size, settings.pos_session_acquire_timeout)
    return assemble(
        settings,
        write_log=SupabaseWriteLog(),
        customers=SupabaseCustomerRepository(),
        sales=SupabaseSaleRepository(),
        items=SupabaseItemRepository(),
        pos_customers=PosCustomerGatewayImpl(pool, settings),
        pos_sales=PosSalesGatewayImpl(pool, settings),
        pool=pool,
    )
