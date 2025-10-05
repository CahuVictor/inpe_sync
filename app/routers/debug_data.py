# app/routers/debug_data.py
from fastapi import APIRouter
from datetime import datetime
from pymongo import UpdateOne

from ..deps import get_mongo

router = APIRouter(prefix="/data/debug", tags=["data-debug"])

@router.get("/coll-info")
async def coll_info():
    """
    Retorna DB/coleção corrente, índices, contagem estimada e exata.
    Útil para confirmar se estamos lendo da mesma coleção onde escrevemos.
    """
    db, coll = await get_mongo()
    idx = await coll.index_information()
    estimated = await coll.estimated_document_count()
    exact = await coll.count_documents({})
    sample = await coll.find_one({}, projection={"_id": 1, "id": 1, "data_hora_gmt": 1})

    return {
        "db": db.name,
        "collection": coll.name,
        "full_name": f"{db.name}.{coll.name}",
        "indexes": list(idx.keys()),
        "count_estimated": int(estimated),
        "count_exact": int(exact),
        "sample": sample,
    }

@router.post("/write-test")
async def write_test():
    _, coll = await get_mongo()
    probe_id = "__write_probe__"
    await coll.update_one({"_id": probe_id}, {"$set": {"ts": datetime.utcnow().isoformat()}}, upsert=True)
    doc = await coll.find_one({"_id": probe_id})
    return {"ok": bool(doc), "doc": doc}

@router.post("/bulk-test")
async def bulk_test():
    _, coll = await get_mongo()
    ops = [
        UpdateOne({"_id": "__bulk_test__1"}, {"$set": {"v": 1}}, upsert=True),
        UpdateOne({"_id": "__bulk_test__2"}, {"$set": {"v": 2}}, upsert=True),
        UpdateOne({"_id": "__bulk_test__3"}, {"$set": {"v": 3}}, upsert=True),
    ]
    res = await coll.bulk_write(ops, ordered=False)
    return {
        "ack": res.acknowledged,
        "matched": res.matched_count,
        "modified": res.modified_count,
        "upserted": len(res.upserted_ids or {}),
    }