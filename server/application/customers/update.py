from __future__ import annotations

from server.application.hashing import request_hash
from server.application.pos_write import PosWriteSaga, WriteResult
from server.domain.customers.models import CustomerFields, CustomerUpdateRequest
from server.domain.customers.ports import CustomerRepository, PosCustomerGateway
from server.domain.errors import NotFoundError
from server.domain.write_log import WriteKind, WriteLogEntry
from pos4africa.shared.utils.sale_ref import ref_from_key


class UpdateCustomerHandler:
    kind = WriteKind.CUSTOMER_UPDATE
    idempotent = True  # re-applying the same field values is harmless

    def __init__(self, gateway: PosCustomerGateway):
        self._gateway = gateway

    @staticmethod
    def _parts(entry: WriteLogEntry) -> tuple[int, CustomerFields]:
        return int(entry.payload["pos_customer_id"]), CustomerFields.model_validate(entry.payload["changes"])

    async def send(self, entry: WriteLogEntry, *, commit: bool) -> int | None:
        pos_id, changes = self._parts(entry)
        return await self._gateway.update(pos_id, changes, commit=commit)

    async def resolve(self, entry: WriteLogEntry) -> int | None:
        return None  # never needed in practice: unknown outcomes on updates are simply retryable


class UpdateCustomer:
    def __init__(self, saga: PosWriteSaga, repo: CustomerRepository):
        self._saga = saga
        self._repo = repo

    async def execute(self, pos_customer_id: int, req: CustomerUpdateRequest, idempotency_key: str) -> WriteResult:
        ref = ref_from_key(WriteKind.CUSTOMER_UPDATE, idempotency_key)
        digest = request_hash(req, pos_customer_id=pos_customer_id)

        if done := await self._saga.replay_if_done(ref, digest):
            return done
        if await self._repo.get(pos_customer_id) is None:
            raise NotFoundError(f"No customer with pos_customer_id={pos_customer_id}")

        return await self._saga.execute(
            kind=WriteKind.CUSTOMER_UPDATE, ref=ref, request_hash=digest,
            payload={"pos_customer_id": pos_customer_id, "changes": req.model_dump(mode="json", exclude_none=True)},
        )
