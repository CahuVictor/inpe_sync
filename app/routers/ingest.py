# app/routers/ingest.py
from fastapi import APIRouter

from ..deps import get_mongo
from ..services.inpe_client import iter_wfs_48h, iter_wfs
from ..repositories.fires_repo import feature_to_doc, upsert_many, max_date
from ..logging_config import get_logger
from ..config import settings
from ..models import IngestResponse

router = APIRouter(prefix="/ingest", tags=["ingest"])
log = get_logger()

@router.post("/initial", response_model=IngestResponse)
async def run_initial_ingest():
    """
    Ingere o intervalo inicial [INITIAL_START, INITIAL_END] usando a camada configurada.
    Requer MongoDB ativo.
    """
    _, coll = await get_mongo()
    batch = []
    async for feat in iter_wfs(settings.initial_start, settings.initial_end):
        batch.append(feature_to_doc(feat))
        if len(batch) >= 2000:
            await upsert_many(coll, batch); batch.clear()
    if batch: 
        await upsert_many(coll, batch)
    return {"status": "ok", "range": [settings.initial_start, settings.initial_end]}

@router.post("/incremental", response_model=IngestResponse)
async def run_incremental(days: int = 7):
    """
    Ingere janela incremental a partir da última data conhecida (`days` anteriores se vazio).
    """
    _, coll = await get_mongo()
    last = await max_date(coll, "date")
    from ..utils.time_windows import window_from_last
    start, end = window_from_last(last, days=days)
    batch = []
    async for feat in iter_wfs(start, end):
        batch.append(feature_to_doc(feat))
        if len(batch) >= 2000:
            await upsert_many(coll, batch); batch.clear()
    if batch: await upsert_many(coll, batch)
    return {"status": "ok", "range": [start, end], "last_seen": last}

@router.post("/48h", response_model=IngestResponse)
async def ingest_48h():
    """
    Ingere/atualiza a janela 48h (camada 48h já recortada no servidor).
    """
    _, coll = await get_mongo()

    total = 0
    batch = []
    async for feat in iter_wfs_48h():
        batch.append(feature_to_doc(feat))
        if len(batch) >= 2000:
            await upsert_many(coll, batch)
            total += len(batch)
            log.info("ingest.batch_committed", count=len(batch))
            batch.clear()
    if batch:
        await upsert_many(coll, batch)
        total += len(batch)
        log.info("ingest.batch_committed", count=len(batch))

    log.info("ingest.done", layer=settings.wfs_typename, total=total)
    return {"status": "ok", "layer": settings.wfs_typename, "total_upserted": total}