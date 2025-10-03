from fastapi import APIRouter

from ..services.inpe_client import fetch_wfs_page_48h_v2
from ..config import settings
from ..logging_config import get_logger
from ..models import WFSSampleResponse

router = APIRouter(prefix="/debug", tags=["debug"])
log = get_logger()

@router.get("/wfs-sample", response_model=WFSSampleResponse)
async def wfs_sample(limit: int = 10):
    """
    Busca uma página de amostra do WFS (sem usar Mongo) e retorna alguns IDs.
    """
    # pega só a 1ª “página” e retorna contagem + alguns IDs
    data = await fetch_wfs_page_48h_v2(start_index=0, count=max(1, min(limit, settings.wfs_page_size)))
    feats = data.get("features", [])
    ids = []
    for f in feats[:limit]:
        props = f.get("properties") or {}
        ids.append(props.get("ID") or props.get("id") or f.get("id"))
    log.info("debug.wfs_sample", got=len(feats), preview=len(ids))
    return {
        "layer": settings.wfs_typename,
        "received": len(feats),
        "preview_ids": ids,
    }
