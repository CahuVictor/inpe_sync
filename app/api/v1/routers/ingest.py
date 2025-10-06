# app/api/v1/routers/ingest.py
from __future__ import annotations
from fastapi import APIRouter, Depends, Query
from typing import Annotated, Any, Dict
from time import perf_counter
from datetime import datetime, timedelta, timezone

from ....core.config import settings
from ....models.schemas import IngestResponse
from ....core.deps import RepoDep, FireDep            # , SessionDep # get_mongo, 
from ....core.logging_config import get_logger
from ....utils.time_windows import iso_date, window_from_last

# from ....services.inpe_client_old import iter_wfs_48h, iter_wfs
# from ....repositories import fires_repo_old
# from ....repositories.fires_repo_old import feature_to_doc, upsert_many, max_date
# from ....repositories.fires_repo_old import upsert_many as real_upsert_many

router = APIRouter(prefix="/ingest", tags=["Ingestion"])
log = get_logger()

def _doc_from_feature(feat: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte um Feature (GeoJSON do WFS) para o documento que salvamos no Mongo.
    Mantém o mesmo shape usado no /ingest/48h.
    """
    props = feat.get("properties") or {}
    geom = feat.get("geometry")
    
    doc_id = props.get("foco_id") or props.get("id_foco_bdq") or feat.get("id")
    if not doc_id:
        # sem identificador estável, descartamos o registro
        return {}
    return {
        "_id": doc_id,
        "id": doc_id,
        "properties": props,
        "geometry": geom,
        "data_hora_gmt": props.get("data_hora_gmt"),
        "longitude": props.get("longitude"),
        "latitude": props.get("latitude"),
        "satelite": props.get("satelite"),
        "municipio": props.get("municipio"),
        "estado": props.get("estado"),
        "pais": props.get("pais"),
        "bioma": props.get("bioma"),
        "frp": props.get("frp"),
    }

@router.post(
    "/initial",
    summary="Ingest a fixed initial date window",
    response_model=IngestResponse,
    responses={200: {"description": "Ingestion completed"}},
)
async def run_initial_ingest(
    repo: RepoDep,
    source: FireDep,
) -> IngestResponse:
    """
    Ingere o intervalo inicial [INITIAL_START, INITIAL_END] usando a fonte configurada.
    Implementação nova usando DI:
    - `source` (FireSource) provê `iter_range(start, end)`
    - `repo` (Repository) executa `upsert_many(docs)`
    """
    start = settings.initial_start
    end = settings.initial_end

    total = 0
    batch: list[dict] = []
    t0 = perf_counter()

    # async for feat in source.iter_range(start, end):
    async for feat in source.iter_range(start, end, typename=settings.wfs_typename_hist):
        doc = _doc_from_feature(feat)
        if not doc:
            continue
        batch.append(doc)
        if len(batch) >= 2000:
            total += await repo.upsert_many(batch)
            batch.clear()

    if batch:
        total += await repo.upsert_many(batch)

    dt = int((perf_counter() - t0) * 1000)
    log.info("ingest.initial.done", total_upserted=total, range=[start, end], duration_ms=dt)

    # Ajuste o payload conforme o seu IngestResponse (mantendo compatibilidade antiga: status + range)
    return IngestResponse(
        status="ok",
        layer=settings.wfs_typename,
        total_upserted=total,
        range=[start, end],
        duration_ms=dt,
    )

@router.post(
    "/incremental",
    summary="Ingest an incremental time window since last known date",
    response_model=IngestResponse,
    responses={200: {"description": "Incremental ingestion completed"}},
)
async def run_incremental(
    repo: RepoDep,
    source: FireDep,
    days: Annotated[int, Query(gt=0, le=90, description="Janela (dias) caso não exista 'last_seen'")] = 7,
) -> IngestResponse:
    """
    Ingere janela incremental a partir da última data conhecida no banco (campo `data_hora_gmt`).
    Se não houver `last_seen`, recua `days` dias a partir de agora.
    """
    # Busca a última data persistida já no shape novo
    # last_seen = await repo.max_date("data_hora_gmt")
    # last_seen só para retorno/diagnóstico
    last_doc = await repo.find_one_sorted(
        query={"data_hora_gmt": {"$ne": None}},
        sort=[("data_hora_gmt", -1)],
        projection={"_id": 0, "data_hora_gmt": 1},
    )
    last_seen = (last_doc or {}).get("data_hora_gmt")

    start, end = window_from_last(last_seen, days=days)

    total = 0
    batch: list[dict] = []
    t0 = perf_counter()

    # async for feat in source.iter_range(start, end):
    async for feat in source.iter_range(start, end, typename=settings.wfs_typename_hist):
        doc = _doc_from_feature(feat)
        if not doc:
            continue
        batch.append(doc)
        if len(batch) >= 2000:
            total += await repo.upsert_many(batch)
            batch.clear()

    if batch:
        total += await repo.upsert_many(batch)

    dt = int((perf_counter() - t0) * 1000)
    log.info(
        "ingest.incremental.done",
        layer=settings.wfs_typename,
        total=total,
        duration_ms=dt,
        start=start,
        end=end,
        last_seen=last_seen,
    )

    return IngestResponse(
        status="ok",
        layer=settings.wfs_typename,
        total_upserted=total,
        range=[start, end],
        last_seen=last_seen,
        duration_ms=dt,
    )

@router.post(
    "/48h",
    summary="Ingest last 48h fire detections from TerraBrasilis",
    response_model=IngestResponse,
    responses={
        200: {"description": "Ingestion completed", "model": IngestResponse},
        500: {"description": "Unexpected error"},
    },
)
async def ingest_48h(
    repo: RepoDep,
    source: FireDep,
    # session: SessionDep,  # exemplo de Depends custom
    dry_run: Annotated[bool, Query(description="Do not write to DB")] = False,
) -> IngestResult:
    """
    Ingere/atualiza a janela 48h (camada 48h já recortada no servidor).
    """
    t0 = perf_counter()
    total = 0
    batch: list[dict] = []

    async for feat in source.iter_48h():
        doc = _doc_from_feature(feat)
        if not doc:
            continue
        batch.append(doc)
        if len(batch) >= 2000:
            if not dry_run:
                total += await repo.upsert_many(batch)
            batch.clear()

    if batch and not dry_run:
        total += await repo.upsert_many(batch)

    dt = int((perf_counter() - t0) * 1000)
    log.info("ingest.done", layer=settings.wfs_typename, total=total, duration_ms=dt)

    return IngestResponse(
        status="ok",
        layer=settings.wfs_typename,
        total_upserted=total,
        duration_ms=dt,
    )