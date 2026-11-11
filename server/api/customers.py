from fastapi import APIRouter, Depends, Query, Response

from server.api.deps import ContainerDep, KeyDep, require_api_key
from server.api.schemas import WriteResponse, write_response
from server.domain.customers.models import CustomerCreateRequest, CustomerUpdateRequest
from server.domain.errors import NotFoundError

router = APIRouter(prefix="/customers", tags=["Customers"], dependencies=[Depends(require_api_key)])


@router.get("")
async def list_customers(
    c: ContainerDep, q: str | None = None,
    limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0),
):
    return await c.customers.search(q, limit, offset)


@router.get("/{pos_customer_id}")
async def get_customer(pos_customer_id: int, c: ContainerDep):
    row = await c.customers.get(pos_customer_id)
    if row is None:
        raise NotFoundError(f"No customer with pos_customer_id={pos_customer_id}")
    return row


@router.post("", response_model=WriteResponse, status_code=201)
async def create_customer(body: CustomerCreateRequest, key: KeyDep, c: ContainerDep, response: Response):
    return write_response(await c.create_customer.execute(body, key), response)


@router.patch("/{pos_customer_id}", response_model=WriteResponse)
async def update_customer(
    pos_customer_id: int, body: CustomerUpdateRequest, key: KeyDep, c: ContainerDep, response: Response
):
    return write_response(await c.update_customer.execute(pos_customer_id, body, key), response)
