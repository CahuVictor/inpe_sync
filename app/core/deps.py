# app/deps.py
from __future__ import annotations
from typing import Annotated, Tuple
from fastapi import Depends
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection, AsyncIOMotorClient
from pymongo.server_api import ServerApi

from ..core.config import settings
from ..repositories.mongo_repo import MongoRepository
from ..services.wfs_service import WfsFireSource
from ..services.protocols import Repository, FireSource
from .db import get_mongo

MongoDep = Annotated[Tuple[AsyncIOMotorDatabase, AsyncIOMotorCollection], Depends(get_mongo)]

def get_repo(mongo: MongoDep) -> Repository:
    db, coll = mongo
    return MongoRepository(coll)

async def get_fire_source() -> FireSource:
    return WfsFireSource()

RepoDep = Annotated[Repository, Depends(get_repo)]
FireDep = Annotated[FireSource, Depends(get_fire_source)]

# Exemplo de “Session” dependência arbitrária para seu caso:
class RequestSession:
    """Contexto leve da requisição (pode carregar request_id, user, etc.)."""
    def __init__(self, request_id: str | None = None) -> None:
        self.request_id = request_id

def session_depends() -> RequestSession:
    return RequestSession()
