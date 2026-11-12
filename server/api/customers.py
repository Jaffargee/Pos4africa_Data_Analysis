from fastapi import APIRouter, Depends, Response

from server.api.deps import ContainerDep, KeyDep, require_api_key
from server.api.schemas import WriteResponse, write_response
from server.domain.customers.models import CustomerCreateRequest, CustomerUpdateRequest

# Write-only: the dashboard reads customers straight from Supabase.
router = APIRouter(prefix="/customers", tags=["Customers"], dependencies=[Depends(require_api_key)])


@router.post("", response_model=WriteResponse, status_code=201)
async def create_customer(body: CustomerCreateRequest, key: KeyDep, c: ContainerDep, response: Response):
    return write_response(await c.create_customer.execute(body, key), response)


@router.patch("/{pos_customer_id}", response_model=WriteResponse)
async def update_customer(
    pos_customer_id: int, body: CustomerUpdateRequest, key: KeyDep, c: ContainerDep, response: Response
):
    return write_response(await c.update_customer.execute(pos_customer_id, body, key), response)
