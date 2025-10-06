# app/services/mock_services.py
from __future__ import annotations
from typing import Dict, Any, Iterable, AsyncIterator, Optional
from .protocols import FireSource, Repository
import asyncio
from datetime import datetime, timezone

class MockFireSource(FireSource):
    async def iter_48h(self, page_size: int = 1000) -> AsyncIterator[Dict[str, Any]]:
        # Emula 3 itens
        items = [
            {"id": "m1", "data_hora_gmt": "2025-10-03T12:00:00Z", "satelite": "TEST", "geometry": {"type": "Point","coordinates":[-50,-10]}},
            {"id": "m2", "data_hora_gmt": "2025-10-03T13:00:00Z", "satelite": "TEST"},
            {"id": "m3", "data_hora_gmt": "2025-10-03T14:00:00Z", "satelite": "TEST"},
        ]
        for it in items:
            await asyncio.sleep(0)
            yield it

class MockRepository(Repository):
    def __init__(self) -> None:
        self._mem: dict[str, Dict[str, Any]] = {}

    async def upsert_many(self, docs: Iterable[Dict[str, Any]]) -> int:
        cnt = 0
        for d in docs:
            _id = d.get("_id") or d.get("id")
            if _id:
                self._mem[_id] = d
                cnt += 1
        return cnt

    async def count(self, flt: Optional[Dict[str, Any]] = None) -> int:
        return len(self._mem)

    async def recent(self, limit: int) -> list[Dict[str, Any]]:
        arr = list(self._mem.values())
        arr.sort(key=lambda x: x.get("data_hora_gmt") or "", reverse=True)
        return arr[:limit]

    async def find(self, flt: Dict[str, Any], limit: int, skip: int, sort: list[tuple[str, int]]) -> list[Dict[str, Any]]:
        arr = list(self._mem.values())
        # filtro simplificado para demo
        return arr[skip:skip+limit]

    async def agg_stats(self) -> Dict[str, Any]:
        total = len(self._mem)
        vals = [x.get("data_hora_gmt") for x in self._mem.values() if x.get("data_hora_gmt")]
        return {
            "total": total,
            "min_data_hora_gmt": min(vals) if vals else None,
            "max_data_hora_gmt": max(vals) if vals else None,
            "by_satelite": [],
        }
