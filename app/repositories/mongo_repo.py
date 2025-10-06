# app/repositories/mongo_repo.py
from __future__ import annotations
from typing import Dict, Any, Iterable, Optional, List, Tuple
from pymongo import UpdateOne
from motor.motor_asyncio import AsyncIOMotorCollection
from ..services.protocols import Repository

class MongoRepository(Repository):
    def __init__(self, coll: AsyncIOMotorCollection) -> None:
        self._coll = coll

    async def upsert_many(self, docs: Iterable[Dict[str, Any]]) -> int:
        docs = list(docs)
        if not docs:
            return 0
        ops = []
        for d in docs:
            _id = d.get("_id") or d.get("id")
            assert _id, "Missing id/_id"
            ops.append(UpdateOne({"_id": _id}, {"$set": d}, upsert=True))
        res = await self._coll.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0) + (res.modified_count or 0)

    async def count(self, flt: Optional[Dict[str, Any]] = None) -> int:
        return await self._coll.count_documents(flt or {})

    async def recent(self, limit: int) -> list[Dict[str, Any]]:
        cur = self._coll.find({}).sort([("data_hora_gmt", -1)]).limit(limit)
        return await cur.to_list(length=limit)

    async def find(self, flt: Dict[str, Any], limit: int, skip: int, sort: List[Tuple[str, int]]) -> list[Dict[str, Any]]:
        cur = self._coll.find(flt).sort(sort).skip(skip).limit(limit)
        return await cur.to_list(length=limit)

    async def agg_stats(self) -> Dict[str, Any]:
        pipeline = [
            {"$group": {
                "_id": None,
                "total": {"$count": {}},
                "min_date": {"$min": "$data_hora_gmt"},
                "max_date": {"$max": "$data_hora_gmt"},
            }},
        ]
        out = await self._coll.aggregate(pipeline).to_list(1)
        if not out:
            return {"total": 0, "min_data_hora_gmt": None, "max_data_hora_gmt": None, "by_satelite": []}

        row = out[0]
        by_sat = await self._coll.aggregate([
            {"$group": {"_id": "$satelite", "count": {"$sum": 1}}},
            {"$project": {"satelite": "$_id", "_id": 0, "count": 1}},
            {"$sort": {"count": -1}},
        ]).to_list(100)

        return {
            "total": row.get("total", 0),
            "min_data_hora_gmt": row.get("min_date"),
            "max_data_hora_gmt": row.get("max_date"),
            "by_satelite": by_sat,
        }
    
    async def find_one_sorted(
        self,
        query: Dict[str, Any],
        sort: List[Tuple[str, int]],
        projection: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        return await self._coll.find_one(query, sort=sort, projection=projection)
