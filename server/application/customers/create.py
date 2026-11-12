from __future__ import annotations

from server.application.hashing import request_hash
from server.application.pos_write import PosWriteSaga, WriteResult
from server.domain.customers.models import CustomerCreateRequest, CustomerFields
from server.domain.customers.ports import CustomerRepository, PosCustomerGateway
from server.domain.errors import ConflictError
from server.domain.write_log import WriteKind, WriteLogEntry
from pos4africa.shared.utils.sale_ref import ref_from_key


class CreateCustomerHandler:
    """Plugs customer creation into the saga."""

    kind = WriteKind.CUSTOMER_CREATE
    idempotent = False  # a second create would make a duplicate customer

    def __init__(self, gateway: PosCustomerGateway):
        self._gateway = gateway

    @staticmethod
    def _fields(entry: WriteLogEntry) -> CustomerFields:
        return CustomerFields.model_validate(entry.payload["fields"])

    async def send(self, entry: WriteLogEntry, *, commit: bool) -> int | None:
        return await self._gateway.create(self._fields(entry), commit=commit)

    async def resolve(self, entry: WriteLogEntry) -> int | None:
        return await self._gateway.find(self._fields(entry))


class CreateCustomer:
    def __init__(self, saga: PosWriteSaga, repo: CustomerRepository):
        self._saga = saga
        self._repo = repo

    async def execute(self, req: CustomerCreateRequest, idempotency_key: str) -> WriteResult:
        ref = ref_from_key(WriteKind.CUSTOMER_CREATE, idempotency_key)
        digest = request_hash(req)

        if done := await self._saga.replay_if_done(ref, digest):
            return done

        fields = req.to_fields()
        if not req.allow_duplicate:
            # Reads Supabase, which trails POS by one ETL run: a best-effort guard, not a guarantee.
            twins = await self._repo.find_by_name(fields.first_name or "", fields.last_name)
            if twins:
                ids = ", ".join(str(t.get("pos_customer_id")) for t in twins[:5])
                raise ConflictError(
                    f"A customer with this name already exists (pos_customer_id: {ids}). "
                    "Resend with allow_duplicate=true if this is a different person."
                )

        return await self._saga.execute(
            kind=WriteKind.CUSTOMER_CREATE, ref=ref, request_hash=digest,
            payload={"fields": fields.model_dump(mode="json")},
        )
