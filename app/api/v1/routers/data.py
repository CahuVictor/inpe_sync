# app/routers/data.py
from __future__ import annotations
from fastapi import APIRouter, Depends, Query
from typing import Annotated
from pymongo import ASCENDING, DESCENDING

from ....models.schemas import (
    StatsResponse,
    FocusItem, FocusListResponse,
    QueryParams
)
from ....core.deps import RepoDep, MongoDep
from ....core.logging_config import get_logger

router = APIRouter(prefix="/data", tags=["Data"])
log = get_logger()

@router.get(
    "/stats",
    summary="Basic collection stats",
    response_model=StatsResponse,
    responses={200: {"description": "Stats aggregated"}}
)
async def stats(
    repo: RepoDep,
):
    """
    Estatísticas gerais:
      - total exato (count_documents)
      - data mínima e máxima (aggregate)
      - contagem por satélite (aggregate)
    """
    raw = await repo.agg_stats()
    return StatsResponse(**raw)

@router.get(
    "/recent",
    summary="List most recent fire focuses (ordered by data_hora_gmt desc)",
    response_model=FocusListResponse,
    responses={200: {"description": "Recent documents returned"}}
)
async def recent(
    repo: RepoDep,
    mongo: MongoDep,
    limit: Annotated[int, Query(gt=0, le=1000, example=20)] = 20,
):
    """
    Retorna os registros mais recentes, ordenados por data_hora_gmt desc.
    Use ?format=geojson para receber FeatureCollection.
    """
    db, coll = mongo
    
    # 2buscar ordenando por data desc
    cur = (
        coll.find(
            {},
            projection={
                "_id": 1,
                "id": 1,
                "geometry": 1,
                "data_hora_gmt": 1,
                "longitude": 1,
                "latitude": 1,
                "satelite": 1,
                "municipio": 1,
                "estado": 1,
                "pais": 1,
                "bioma": 1,
                "frp": 1,
            },
        )
        .sort([("data_hora_gmt", DESCENDING)])
        .limit(int(limit))
    )
    
    docs = await cur.to_list(length=int(limit))
    
    # mapear para o modelo de saída
    items = [
        FocusItem(
            id=(d.get("id") or str(d.get("_id"))),
            data_hora_gmt=d.get("data_hora_gmt"),
            longitude=d.get("longitude"),
            latitude=d.get("latitude"),
            satelite=d.get("satelite"),
            municipio=d.get("municipio"),
            estado=d.get("estado"),
            pais=d.get("pais"),
            bioma=d.get("bioma"),
            frp=d.get("frp"),
            geometry=d.get("geometry"),
        )
        for d in docs
    ]

    return FocusListResponse(total=len(items), returned=len(items), items=items)

@router.get(
    "/find",
    summary="Find focus documents by filters",
    response_model=FocusListResponse,
    responses={200: {"description": "Filtered documents returned"}}
)
async def find(
    repo: RepoDep,
    q: Annotated[QueryParams, Depends()]
):
    """
    Busca com filtros (temporais, atributos, geoespacial), paginação e formato.
    Ex.: /data/find?start=2025-10-02&end=2025-10-04&estado=Piauí&limit=50
         /data/find?near_lon=-42.5&near_lat=-7.76&near_km=25
         /data/find?bbox=-43.0,-8.0,-42.0,-7.5&format=geojson
    """
    flt: dict = {}
    if q.satelite: flt["satelite"] = q.satelite
    if q.estado: flt["estado"] = q.estado
    if q.municipio: flt["municipio"] = q.municipio
    if q.bioma: flt["bioma"] = q.bioma
    if q.start or q.end:
        rng = {}
        if q.start: rng["$gte"] = q.start
        if q.end: rng["$lte"] = q.end
        flt["data_hora_gmt"] = rng

    sort = [("data_hora_gmt", -1 if q.sort.startswith("-") else 1)]
    items = await repo.find(flt, limit=q.limit, skip=q.skip, sort=sort)
    total = len(items)  # simplificação (poderia contar real)
    
    return FocusListResponse(total=total, returned=len(items), items=items)