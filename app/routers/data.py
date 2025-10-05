# app/routers/data.py
from typing import Dict, Any, List, Optional
from fastapi import APIRouter, Query, HTTPException
from pymongo import ASCENDING, DESCENDING

from ..deps import get_mongo
from ..logging_config import get_logger
from ..models import (
    StatsResponse, SatelliteCount,
    FocusItem, FocusListResponse,
    QueryParams, GeoJSONFeature, GeoJSONFeatureCollection
)

router = APIRouter(prefix="/data", tags=["data"])
log = get_logger()

def _to_feature(doc: Dict[str, Any]) -> GeoJSONFeature:
    """Converte doc de foco para GeoJSON Feature."""
    props = {
        "id": doc.get("id"),
        "data_hora_gmt": doc.get("data_hora_gmt"),
        "longitude": doc.get("longitude"),
        "latitude": doc.get("latitude"),
        "satelite": doc.get("satelite"),
        "municipio": doc.get("municipio"),
        "estado": doc.get("estado"),
        "pais": doc.get("pais"),
        "bioma": doc.get("bioma"),
        "frp": doc.get("frp"),
    }
    return GeoJSONFeature(
        id=str(doc.get("_id") or doc.get("id")),
        geometry=doc.get("geometry"),
        properties={k: v for k, v in props.items() if v is not None},
    )

def _to_item(doc: Dict[str, Any]) -> FocusItem:
    """Converte doc de foco para modelo simplificado."""
    return FocusItem(
        id=doc.get("id") or str(doc.get("_id")),
        data_hora_gmt=doc.get("data_hora_gmt"),
        longitude=doc.get("longitude"),
        latitude=doc.get("latitude"),
        satelite=doc.get("satelite"),
        municipio=doc.get("municipio"),
        estado=doc.get("estado"),
        pais=doc.get("pais"),
        bioma=doc.get("bioma"),
        frp=doc.get("frp"),
        geometry=doc.get("geometry"),
    )

def _build_filters(q: QueryParams) -> Dict[str, Any]:
    """Monta o dicionário de filtros para o Mongo."""
    filt: Dict[str, Any] = {}

    # filtro temporal (strings ISO 'YYYY-MM-DDTHH:MM:SSZ' funcionam lexicograficamente)
    if q.start or q.end:
        rng: Dict[str, Any] = {}
        if q.start:
            rng["$gte"] = f"{q.start}T00:00:00Z"
        if q.end:
            rng["$lte"] = f"{q.end}T23:59:59Z"
        filt["data_hora_gmt"] = rng

    if q.satelite:
        filt["satelite"] = q.satelite
    if q.estado:
        filt["estado"] = q.estado
    if q.municipio:
        filt["municipio"] = q.municipio
    if q.bioma:
        filt["bioma"] = q.bioma

    # espacial
    if q.near_lon is not None and q.near_lat is not None and q.near_km:
        filt["geometry"] = {
            "$near": {
                "$geometry": {"type": "Point", "coordinates": [q.near_lon, q.near_lat]},
                "$maxDistance": float(q.near_km) * 1000.0,
            }
        }
    elif q.bbox:
        try:
            minLon, minLat, maxLon, maxLat = [float(x) for x in q.bbox.split(",")]
        except Exception:
            raise HTTPException(status_code=400, detail="bbox inválido. Use minLon,minLat,maxLon,maxLat")
        filt["geometry"] = {
            "$geoWithin": {
                "$geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [minLon, minLat],
                        [maxLon, minLat],
                        [maxLon, maxLat],
                        [minLon, maxLat],
                        [minLon, minLat]
                    ]]
                }
            }
        }

    return filt

def _sort_tuple(sort_param: str):
    if sort_param.startswith("-"):
        return [("data_hora_gmt", DESCENDING)]
    return [("data_hora_gmt", ASCENDING)]

@router.get("/stats", response_model=StatsResponse)
async def stats():
    """
    Estatísticas gerais:
      - total exato (count_documents)
      - data mínima e máxima (aggregate)
      - contagem por satélite (aggregate)
    """
    _, coll = await get_mongo()
    
    total_exact = await coll.count_documents({})

    pipeline = [
        {
            "$facet": {
                "range": [
                    {"$group": {
                        "_id": None,
                        # "total": {"$sum": 1},
                        "min": {"$min": "$data_hora_gmt"},
                        "max": {"$max": "$data_hora_gmt"},
                    }}
                ],
                "by_satelite": [
                    {"$group": {"_id": "$satelite", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]
            }
        }
    ]

    res = await coll.aggregate(pipeline).to_list(1)
    # if not res:
    #     return StatsResponse(total=0, min_data_hora_gmt=None, max_data_hora_gmt=None, by_satelite=[])

    # facet = res[0]
    # rng = (facet.get("range") or [{}])[0] if facet.get("range") else {}
    # by_sat = facet.get("by_satelite") or []
    
    rng = {}
    by_sat = []
    if res:
        facet = res[0]
        rng = (facet.get("range") or [{}])[0] if facet.get("range") else {}
        by_sat = facet.get("by_satelite") or []

    return StatsResponse(
        # total=int(rng.get("total", 0) or 0),
        total=total_exact,
        min_data_hora_gmt=rng.get("min"),
        max_data_hora_gmt=rng.get("max"),
        by_satelite=[SatelliteCount(satelite=row.get("_id"), count=row.get("count", 0)) for row in by_sat],
    )

@router.get("/recent", response_model=FocusListResponse)
async def recent(
    limit: int = Query(20, gt=0, le=1000),
    format: str = Query("json", pattern="^(json|geojson)$")
):
    """
    Retorna os registros mais recentes, ordenados por data_hora_gmt desc.
    Use ?format=geojson para receber FeatureCollection.
    """
    _, coll = await get_mongo()
    cur = coll.find({}, projection={"_id": 1, "id": 1, "geometry": 1,
                                    "data_hora_gmt": 1, "longitude": 1, "latitude": 1,
                                    "satelite": 1, "municipio": 1, "estado": 1, "pais": 1,
                                    "bioma": 1, "frp": 1}
                   ).sort([("data_hora_gmt", DESCENDING)]
                   ).limit(int(limit))
    docs = await cur.to_list(limit)

    if format == "geojson":
        fc = GeoJSONFeatureCollection(features=[_to_feature(d) for d in docs])
        # em GeoJSON devolvemos só a coleção; se preferir embalar com total/returned, mude o model
        return FocusListResponse(total=len(docs), returned=len(docs), items=[_to_item(d) for d in docs])

    return FocusListResponse(total=len(docs), returned=len(docs), items=[_to_item(d) for d in docs])

@router.get("/find")
async def find(q: QueryParams = Query(...)):
    """
    Busca com filtros (temporais, atributos, geoespacial), paginação e formato.
    Ex.: /data/find?start=2025-10-02&end=2025-10-04&estado=Piauí&limit=50
         /data/find?near_lon=-42.5&near_lat=-7.76&near_km=25
         /data/find?bbox=-43.0,-8.0,-42.0,-7.5&format=geojson
    """
    _, coll = await get_mongo()
    filt = _build_filters(q)
    sort = _sort_tuple(q.sort)

    projection = {
        "_id": 1, "id": 1, "geometry": 1,
        "data_hora_gmt": 1, "longitude": 1, "latitude": 1,
        "satelite": 1, "municipio": 1, "estado": 1, "pais": 1,
        "bioma": 1, "frp": 1
    }

    cursor = coll.find(filt, projection=projection).sort(sort).skip(int(q.skip)).limit(int(q.limit))
    docs = await cursor.to_list(length=q.limit)

    if q.format == "geojson":
        features = [_to_feature(d) for d in docs]
        return GeoJSONFeatureCollection(features=features)

    # formato json
    items = [_to_item(d) for d in docs]
    # obter total estimado (rápido) – para contagem exata poderia usar count_documents(filt)
    total = await coll.estimated_document_count()
    return {
        "total": int(total),
        "returned": len(items),
        "items": [i.dict() for i in items],
    }