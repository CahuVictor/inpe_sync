# app/deps.py
from pymongo.server_api import ServerApi
from motor.motor_asyncio import AsyncIOMotorClient

from .config import settings
from .logging_config import get_logger

log = get_logger()

_mongo_client: AsyncIOMotorClient | None = None
_db = None
_coll = None

async def get_mongo():
    """
    Obtém cliente/conexão MongoDB (singleton) e garante índices:
      - id (único) para upsert idempotente,
      - geometry (2dsphere) para consultas geoespaciais,
      - data_hora_gmt (asc) para ordenações por data.
    """
    global _mongo_client, _db, _coll
    if _mongo_client is None:
        if not settings.mongodb_uri:
            raise RuntimeError("MONGODB_URI não configurado (.env)")
        log.info("mongo.connecting", uri_hint=settings.mongodb_uri.split("@")[-1])
        _mongo_client = AsyncIOMotorClient(
            settings.mongodb_uri,
            server_api=ServerApi('1'),     # Passe ServerApi('1') como no snippet do Atlas - Se esse ping falhar, você verá o erro de auth na linha do ping (fica bem mais direto para diagnosticar).
            serverSelectionTimeoutMS=10000,
        )
        _db = _mongo_client[settings.mongodb_db]
        _coll = _db[settings.mongodb_coll]
        
        # índices — use 'id' como chave única (BDQueimadas foca em ID único)
        # await _coll.create_index("gid", unique=True)
        # await _coll.create_index([(settings.wfs_date_field, 1)]) # índice por data para queries incrementais
        await _coll.create_index("id", unique=True)             # chave única
        await _coll.create_index([("geometry", "2dsphere")])    # consultas geo
        await _coll.create_index([("view_date", 1)])            # se existir
        
        log.info("mongo.connected",
                 db=settings.mongodb_db,
                 coll=settings.mongodb_coll)
    return _db, _coll
