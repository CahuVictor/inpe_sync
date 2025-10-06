from fastapi import APIRouter

# from ....models import HealthResponse

router = APIRouter(prefix="/health", tags=["Health"])

@router.get(
    "",
    summary="Healthcheck",
    # response_model=HealthResponse,
    responses={200: {"description": "OK"}}
)
async def health():
    """
    Healthcheck básico (sempre ok=True). Para checks profundos,
    acrescente validação de conexão ao Mongo/WFS.
    """
    return {"ok": True}
