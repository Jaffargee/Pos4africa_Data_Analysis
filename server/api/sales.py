from fastapi import APIRouter, Depends, Response

from server.api.deps import ContainerDep, KeyDep, require_api_key
from server.api.schemas import WriteResponse, write_response
from server.domain.sales.models import CreateSaleRequest

# Write-only: the dashboard reads sales straight from Supabase.
router = APIRouter(prefix="/sales", tags=["Sales"], dependencies=[Depends(require_api_key)])


@router.post("", response_model=WriteResponse, status_code=201)
async def create_sale(body: CreateSaleRequest, key: KeyDep, c: ContainerDep, response: Response):
    return write_response(await c.create_sale.execute(body, key), response)
