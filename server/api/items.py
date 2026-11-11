from fastapi import APIRouter, Depends, Query

from server.api.deps import ContainerDep, require_api_key

router = APIRouter(prefix="/items", tags=["Items"], dependencies=[Depends(require_api_key)])


@router.get("")
async def search_items(c: ContainerDep, q: str | None = None, limit: int = Query(50, ge=1, le=200)):
    return await c.items.search(q, limit)
