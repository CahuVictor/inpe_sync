# app/routers/ingest.py
from fastapi import APIRouter, Query
from datetime import datetime

from ..deps import get_mongo
from ..services.inpe_client import iter_wfs_48h, iter_wfs
from ..repositories import fires_repo
from ..repositories.fires_repo import feature_to_doc, upsert_many, max_date
from ..repositories.fires_repo import upsert_many as real_upsert_many
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
# async def ingest_48h():
async def ingest_48h(dry_run: bool = Query(False), mock_write: bool = Query(False)):
    """
    Ingere/atualiza a janela 48h (camada 48h já recortada no servidor).
    """
    # _, coll = await get_mongo()
    db, coll = await get_mongo()
    
    # loga qual implementação está em uso
    log.info(
        "ingest.repo_impl",
        func=f"{real_upsert_many.__module__}.{real_upsert_many.__name__}",
        dry_run=dry_run,
        mock_write=mock_write,
        coll=str(coll.full_name),
    )
    
    # opção de mock: substitui upsert_many só dentro desta requisição
    async def _mock_upsert_many(c, docs):
        # não grava todos os docs — grava só uma sonda e conta quantos passaram
        await c.update_one(
            {"_id": "__mock_48h__"},
            {"$inc": {"calls": 1, "docs_seen": len(list(docs))}, "$set": {"ts": datetime.utcnow().isoformat()}},
            upsert=True,
        )
        log.info("MOCK.upsert_many", docs_seen=len(list(docs)))

    upsert_fn = _mock_upsert_many if mock_write else real_upsert_many

    total = 0
    batch = []
    async for feat in iter_wfs_48h():
        if dry_run:
            total += 1
            continue
        
        batch.append(feature_to_doc(feat))
        if len(batch) >= 2000:
            # await upsert_many(coll, batch)
            await upsert_fn(coll, batch)
            total += len(batch)
            log.info("ingest.batch_processed", count=len(batch))
            batch.clear()
    # if batch:
    if not dry_run and batch:
        # await upsert_many(coll, batch)
        await upsert_fn(coll, batch)
        total += len(batch)
        log.info("ingest.batch_committed", count=len(batch))

    if dry_run:
        return {"status": "dry-run", "would_upsert": total}
    
    log.info("ingest.done", layer=settings.wfs_typename, total=total)
    
    exact = await coll.count_documents({})
    log.info("ingest.collection_size", coll=str(coll.full_name), total_exact=exact)

    # return {"status": "ok", "layer": settings.wfs_typename, "total_upserted": total}
    return {"status": "ok", "layer": settings.wfs_typename, "total_upserted": total, "collection_size": exact}