from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from server.application.hashing import request_hash
from server.application.pos_write import PosWriteSaga, WriteResult
from server.domain.customers.ports import CustomerRepository
from server.domain.errors import NotFoundError, PosOutcomeUnknownError, ValidationFailed
from server.domain.sales.models import (
    PAYMENT_TOLERANCE,
    CreateSaleRequest,
    ResolvedLine,
    ResolvedPayment,
    ResolvedSale,
    compute_fingerprint,
    money,
)
from server.domain.sales.ports import ItemRepository, PosSalesGateway, SaleRepository
from server.domain.write_log import WriteKind, WriteLogEntry
from pos4africa.shared.utils.sale_ref import attach_ref, ref_from_key


class SaleHandler:
    kind = WriteKind.SALE_CREATE
    idempotent = False

    def __init__(self, gateway: PosSalesGateway, repo: SaleRepository):
        self._gateway = gateway
        self._repo = repo

    @staticmethod
    def _sale(entry: WriteLogEntry) -> ResolvedSale:
        return ResolvedSale.model_validate(entry.payload["sale"])

    async def _find_verified(self, entry: WriteLogEntry, sale: ResolvedSale) -> int | None:
        found = await self._gateway.find_by_ref(sale.ref, since=entry.created_at)
        if found is None:
            return None
        if found.fingerprint(sale.ref) != sale.fingerprint:
            raise PosOutcomeUnknownError(
                f"A POS sale (code {found.pos_sale_id}) carries ref {sale.ref} but its contents differ "
                "from what was requested. Needs a human look; nothing was changed locally."
            )
        return found.pos_sale_id

    async def send(self, entry: WriteLogEntry, *, commit: bool) -> int | None:
        sale = self._sale(entry)
        await self._gateway.create(sale, commit=commit)
        if not commit:
            return None
        pos_id = await self._find_verified(entry, sale)
        if pos_id is None:
            raise PosOutcomeUnknownError("Sale was completed in POS but could not be read back")
        return pos_id

    async def resolve(self, entry: WriteLogEntry) -> int | None:
        return await self._find_verified(entry, self._sale(entry))

    async def commit_local(self, entry: WriteLogEntry, pos_id: int) -> None:
        await self._repo.commit_sale(pos_id, self._sale(entry))


class CreateSale:
    def __init__(
        self,
        saga: PosWriteSaga,
        sales: SaleRepository,
        customers: CustomerRepository,
        items: ItemRepository,
        *,
        walk_in_customer_id: int,
        salesperson: str,
    ):
        self._saga = saga
        self._sales = sales
        self._customers = customers
        self._items = items
        self._walk_in = walk_in_customer_id
        self._salesperson = salesperson

    async def execute(self, req: CreateSaleRequest, idempotency_key: str) -> WriteResult:
        ref = ref_from_key(WriteKind.SALE_CREATE, idempotency_key)
        digest = request_hash(req)

        if done := await self._saga.replay_if_done(ref, digest):
            return done

        sale = await self._resolve(req, ref)
        return await self._saga.execute(
            kind=WriteKind.SALE_CREATE, ref=ref, request_hash=digest,
            payload={"sale": sale.model_dump(mode="json")},
        )

    async def _resolve(self, req: CreateSaleRequest, ref: str) -> ResolvedSale:
        # customer
        if req.pos_customer_id is not None:
            row = await self._customers.get(req.pos_customer_id)
            if row is None:
                raise NotFoundError(f"No customer with pos_customer_id={req.pos_customer_id}")
            customer_id = req.pos_customer_id
            customer_name = row.get("company_name") or row.get("customer_name") or f"Customer #{customer_id}"
        else:
            customer_id, customer_name = self._walk_in, "Walk-in Customer"

        # lines
        items = await self._items.get_many([line.pos_item_id for line in req.lines])
        lines: list[ResolvedLine] = []
        for line in req.lines:
            item = items.get(line.pos_item_id)
            if item is None:
                raise ValidationFailed(f"Unknown item pos_item_id={line.pos_item_id}")
            if item.get("inactive"):
                raise ValidationFailed(f"Item {line.pos_item_id} ({item.get('item_name')}) is inactive")
            price = line.unit_price if line.unit_price is not None else item.get("selling_price")
            if price is None:
                raise ValidationFailed(f"Item {line.pos_item_id} has no selling price; pass unit_price")
            price = money(Decimal(str(price)))
            lines.append(ResolvedLine(
                pos_item_id=line.pos_item_id, name=item["item_name"], quantity=line.quantity,
                unit_price=price, total=money(line.quantity * price),
                cost_price=None if item.get("cost_price") is None else Decimal(str(item["cost_price"])),
            ))
        total = money(sum((ln.total for ln in lines), Decimal("0")))

        # payments
        accounts = await self._sales.get_accounts([p.account_id for p in req.payments])
        payments: list[ResolvedPayment] = []
        for pay in req.payments:
            name = accounts.get(pay.account_id)
            if name is None:
                raise ValidationFailed(f"Unknown payment account {pay.account_id}")
            payments.append(ResolvedPayment(account_id=pay.account_id, account=name, amount=money(pay.amount)))
        paid = sum((p.amount for p in payments), Decimal("0"))
        if abs(paid - total) > PAYMENT_TOLERANCE:
            raise ValidationFailed(f"Payments ({paid}) do not match the invoice total ({total})")

        comment = (req.comment or "").strip() or None
        return ResolvedSale(
            ref=ref, pos_customer_id=customer_id, customer_name=customer_name, salesperson=self._salesperson,
            comment=comment, comment_with_ref=attach_ref(comment, ref),
            lines=lines, payments=payments, total=total,
            fingerprint=compute_fingerprint(customer_id, [(ln.pos_item_id, ln.quantity, ln.unit_price) for ln in lines], total, ref),
        )
