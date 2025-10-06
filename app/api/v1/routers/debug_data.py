# app/api/v1/routers/debug_data.py
from fastapi import APIRouter
from pymongo import UpdateOne
import httpx, re

from ....core.logging_config import get_logger
from ....core.db import get_mongo as _get_mongo_original
from ....models.schemas import WFSSchemaResponse
from ....core.config import settings
from ....services.inpe_client_old import BASE, SERVICE

log = get_logger()

router = APIRouter(prefix="/data/debug", tags=["data-debug"])

@router.get("/coll-info")
async def coll_info():
    """
    Retorna DB/coleção corrente, índices, contagem estimada e exata.
    Útil para confirmar se estamos lendo da mesma coleção onde escrevemos.
    """
    db, coll = await _get_mongo_original()
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

@router.post(
    "/write-test",
    summary="Escreve um doc de teste na mesma coleção de produção (com id)"
)
async def write_test():
    _, coll = await _get_mongo_original()
    doc_id = "__debug__" # probe_id = "__write_probe__"
    doc = {"_id": doc_id, "id": doc_id, "ok": True}
    await coll.update_one({"_id": doc_id}, {"$set": doc}, upsert=True)
    return {"ok": True}

@router.post(
    "/bulk-test",
    summary="Teste de bulk_write com upsert (respeita índice único em id)"
)
async def bulk_test():
    _, coll = await _get_mongo_original()
    ops = []
    for i in range(1, 3 + 1):
        _id = f"__bulk_test__{i}"
        ops.append(
            UpdateOne(
                {"_id": _id},
                {"$set": {"id": _id, "v": i}},  # <- garante 'id' não-nulo/único
                upsert=True,
            )
        )
    res = await coll.bulk_write(ops, ordered=False)
    return {
        "ack": res.acknowledged,
        "matched": res.matched_count,
        "modified": res.modified_count,
        "upserted": len(res.upserted_ids or {}),
    }

@router.post(
    "/fix-legacy-null-id",
    summary="Ajusta documentos antigos sem 'id'"
)
async def fix_legacy_null_id():
    _, coll = await _get_mongo_original()
    await coll.update_one({"_id": "__write_probe__"}, {"$set": {"id": "__write_probe__"}})
    exact = await coll.count_documents({})
    return {
        "fixed_probe": True,
        "count_exact": int(exact)
    }

@router.get(
    "/wfs-schema",
    response_model=WFSSchemaResponse
)
async def wfs_schema():
    """
    Retorna os atributos de `settings.wfs_typename` usando WFS DescribeFeatureType.
    Útil para descobrir campos válidos para filtros/ordenção (sortBy).
    """
    # DescribeFeatureType: WFS 2.0 usa 'typeNames'
    url = f"{BASE}{SERVICE}?service=WFS&version=2.0.0&request=DescribeFeatureType&typeNames={settings.wfs_typename}"
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url)
        # Em alguns servidores, o retorno é XML/XSD (texto)
        r.raise_for_status()
        xsd = r.text

    # parse leve: pega <xsd:element name="...">
    names = re.findall(r'<xsd:element[^>]*name="([^"]+)"', xsd)
    # remove duplicados e ordena
    seen = set(); attrs=[]
    for n in names:
        if n not in seen:
            seen.add(n); attrs.append(n)

    log.info("wfs.schema_attrs", count=len(attrs))
    # devolve só um pedaço do XSD pra não pesar
    return {"typeNames": settings.wfs_typename, "attr_count": len(attrs), "attributes": attrs[:200], "xsd_snippet": xsd[:1500]}