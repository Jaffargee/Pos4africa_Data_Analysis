from fastapi import APIRouter, Depends, Query

from server.api.deps import ContainerDep, require_api_key
from server.api.schemas import ReconEntry
from server.domain.errors import NotFoundError
from server.domain.write_log import WriteKind, WriteStatus

router = APIRouter(prefix="/recon", tags=["Reconciliation"], dependencies=[Depends(require_api_key)])


@router.get("", response_model=list[ReconEntry])
async def list_writes(
    c: ContainerDep, status: WriteStatus | None = None, kind: WriteKind | None = None,
    limit: int = Query(50, ge=1, le=200),
):
    return [ReconEntry.of(e) for e in await c.write_log.list(status=status, kind=kind, limit=limit)]


@router.get("/{ref}", response_model=ReconEntry)
async def get_write(ref: str, c: ContainerDep):
    entry = await c.write_log.get_by_ref(ref)
    if entry is None:
        raise NotFoundError(f"No write-log entry with ref {ref}")
    return ReconEntry.of(entry)


@router.post("/reconcile", response_model=list[ReconEntry])
async def reconcile_all(c: ContainerDep):
    """Resolve every stuck write older than the grace period."""
    return [ReconEntry.of(e) for e in await c.reconciler.all_open()]


@router.post("/{ref}/reconcile", response_model=ReconEntry)
async def reconcile_one(ref: str, c: ContainerDep):
    return ReconEntry.of(await c.reconciler.one(ref))
