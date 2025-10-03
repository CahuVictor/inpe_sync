from fastapi import APIRouter

from ..models import HealthResponse

router = APIRouter(prefix="/health", tags=["health"])

@router.get("/health", response_model=HealthResponse)
async def health():
    """
    Healthcheck básico (sempre ok=True). Para checks profundos,
    acrescente validação de conexão ao Mongo/WFS.
    """
    return {"ok": True}
