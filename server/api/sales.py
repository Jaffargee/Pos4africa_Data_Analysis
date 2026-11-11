from fastapi import APIRouter, Depends, Query, Response

from server.api.deps import ContainerDep, KeyDep, require_api_key
from server.api.schemas import WriteResponse, write_response
from server.domain.errors import NotFoundError
from server.domain.sales.models import CreateSaleRequest

router = APIRouter(prefix="/sales", tags=["Sales"], dependencies=[Depends(require_api_key)])


@router.get("")
async def list_sales(
    c: ContainerDep, pos_customer_id: int | None = None,
    limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0),
):
    return await c.sales.list(pos_customer_id, limit, offset)


@router.get("/{pos_sale_id}")
async def get_sale(pos_sale_id: int, c: ContainerDep):
    row = await c.sales.get(pos_sale_id)
    if row is None:
        raise NotFoundError(f"No sale with pos_sale_id={pos_sale_id}")
    return row


@router.post("", response_model=WriteResponse, status_code=201)
async def create_sale(body: CreateSaleRequest, key: KeyDep, c: ContainerDep, response: Response):
    return write_response(await c.create_sale.execute(body, key), response)
