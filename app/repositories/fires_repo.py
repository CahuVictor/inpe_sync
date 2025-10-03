from typing import Any, Dict, Iterable
from motor.motor_asyncio import AsyncIOMotorCollection

from ..logging_config import get_logger

log = get_logger()

def feature_to_doc(f: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converte uma feature GeoJSON do WFS (48h) para documento MongoDB.
    - Chave: foco_id (string) ou id_foco_bdq (long); fallback para hash.
    - Mantém geometry conforme vem (EPSG:4326).
    """
    props = f.get("properties", {}) or {}
    geom = f.get("geometry", {}) or {}
    
    # chave: foco_id ou id_foco_bdq
    # gid = props.get("gid") or f.get("id")
    # doc_id = props.get("id") or f.get("id")  # fallback
    doc_id = props.get("foco_id") or props.get("id_foco_bdq") or f.get("id")
    
    if not doc_id:
        # fallback: hash determinístico de campos estáveis
        import hashlib, json
        doc_id = hashlib.sha1(json.dumps([geom, props.get("view_date")], sort_keys=True).encode()).hexdigest()

    # return {
    #     "_id": doc_id, # gid,                # espelha gid no _id para upsert simples
    #     "id": doc_id, # gid,
    #     "properties": props,
    #     "geometry": geom,
    #     # campo de data duplicado para facilitar queries (ajuste ao nome)
    #     # "date": props.get("date") or props.get("data") or props.get("datetime"),
    #     # "view_date": props.get("view_date") or props.get("date") or props.get("data") or props.get("datetime"),
    #     "view_date": props.get("view_date") or props.get("DataHora") or props.get("datetime"),
    # }
    return {
        "_id": doc_id,
        "id": doc_id,
        "properties": props,
        "geometry": geom,
        "data_hora_gmt": props.get("data_hora_gmt"),  # datetime
        "longitude": props.get("longitude"),
        "latitude": props.get("latitude"),
        "satelite": props.get("satelite"),
        "municipio": props.get("municipio"),
        "estado": props.get("estado"),
        "pais": props.get("pais"),
        "bioma": props.get("bioma"),
        "frp": props.get("frp"),
    }

async def upsert_many(coll: AsyncIOMotorCollection, docs: Iterable[Dict[str, Any]]):
    """
    Executa bulk upsert (idempotente) para uma lista de documentos.
    """
    docs = list(docs)
    if not docs:
        return
    ops = []
    for d in docs:
        key = {"_id": d["_id"]}
        ops.append(
            {
                "updateOne": {
                    "filter": key,
                    "update": {"$set": d},
                    "upsert": True,
                }
            }
        )
    # if ops:
    #     await coll.bulk_write(ops, ordered=False)
    res = await coll.bulk_write(ops, ordered=False)
    log.info("mongo.bulk_upsert", matched=res.matched_count, upserted=len(res.upserted_ids or []), modified=res.modified_count)

async def max_date(coll: AsyncIOMotorCollection, date_field: str) -> str | None:
    """
    Retorna a maior data (mais recente) encontrada em `date_field`.
    """
    doc = await coll.find({date_field: {"$exists": True}}).sort([(date_field, -1)]).limit(1).to_list(1)
    if doc:
        return doc[0].get(date_field)
    return None
