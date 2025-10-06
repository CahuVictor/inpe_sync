from fastapi import APIRouter

from .health import router as health
from .ingest import router as ingest
from .data import router as data
from .debug_data import router as debug_data

api = APIRouter()
api.include_router(health)
api.include_router(ingest)
api.include_router(data)
api.include_router(debug_data)