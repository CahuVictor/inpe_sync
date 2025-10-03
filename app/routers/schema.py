from fastapi import APIRouter
import httpx, re

from ..config import settings
from ..services.inpe_client import BASE, SERVICE
from ..logging_config import get_logger
from ..models import WFSSchemaResponse

router = APIRouter(prefix="/debug", tags=["debug"])
log = get_logger()

@router.get("/wfs-schema", response_model=WFSSchemaResponse)
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
