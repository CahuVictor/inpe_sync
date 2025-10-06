# app/core/db.py
from __future__ import annotations
import os
from typing import Tuple
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo.server_api import ServerApi

from .config import settings
from .logging_config import get_logger

log = get_logger()

_mongo_client: AsyncIOMotorClient | None = None
_db: AsyncIOMotorDatabase | None = None
_coll: AsyncIOMotorCollection | None = None


async def get_mongo() -> Tuple[AsyncIOMotorDatabase, AsyncIOMotorCollection]:
    """
    Retorna (db, coll) do MongoDB como singleton e garante índices:
      - 'id' (único) para upsert idempotente,
      - 'geometry' (2dsphere) para consultas geoespaciais,
      - 'data_hora_gmt' (asc) para ordenações/consultas temporais.
    Executa também um 'ping' para falhas aparecerem cedo (auth/dns/etc).
    """
    global _mongo_client, _db, _coll

    if _mongo_client is None:
        if not settings.mongodb_uri:
            raise RuntimeError("MONGODB_URI não configurado (.env)")

        # log seguro (sem credenciais)
        uri_hint = settings.mongodb_uri.split("@")[-1] if "@" in settings.mongodb_uri else settings.mongodb_uri
        log.info("mongo.connecting", uri_hint=uri_hint)

        _mongo_client = AsyncIOMotorClient(
            settings.mongodb_uri,
            server_api=ServerApi("1"),     # segue o snippet do Atlas (propaga via Motor -> PyMongo)
            serverSelectionTimeoutMS=10_000,
        )

        # ping cedo para falhas aparecerem já no startup/primeira chamada
        await _mongo_client.admin.command("ping")

        _db = _mongo_client[settings.mongodb_db]
        _coll = _db[settings.mongodb_coll]

        # Índices
        await _coll.create_index("id", unique=True)             # chave única
        await _coll.create_index([("geometry", "2dsphere")])    # geo
        await _coll.create_index([("data_hora_gmt", 1)])        # data

        log.info("mongo.connected",
                 db=settings.mongodb_db,
                 coll=settings.mongodb_coll
        )

    # tipos ignorados porque mypy não entende o guard anterior
    return _db, _coll  # type: ignore[return-value]