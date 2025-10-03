# app/services/inpe_client.py
from typing import AsyncIterator, Dict, Any, Optional
from urllib.parse import urlencode
import httpx
from httpx import HTTPStatusError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pybreaker
import re

from ..config import settings
from ..logging_config import get_logger

log = get_logger()

BASE = settings.wfs_base.rstrip("/")
SERVICE = settings.wfs_service_path  # ex.: "/wfs"

# circuit breaker para chamadas ao WFS
wfs_breaker = pybreaker.CircuitBreaker(
    fail_max=settings.breaker_fail_max,
    reset_timeout=settings.breaker_reset_timeout,
)

def _wfs_url(params: dict) -> str:
    return f"{BASE}{SERVICE}?{urlencode(params)}"

def _clean_sortby(val: str | None) -> str | None:
    if not val:
        return None
    # remove comentários/“lixos” após '#'
    v = val.split("#", 1)[0].strip()
    return v or None

# async def fetch_wfs_page(
#     start: str,
#     end: str,
#     start_index: int = 0,
#     count: int = settings.wfs_page_size,
# ) -> dict:
#     params = {
#         "service": "WFS",
#         "version": "2.0.0",
#         "request": "GetFeature",
#         "typeName": settings.wfs_typename,
#         "srsName": settings.wfs_srid,
#         "outputFormat": "application/json",
#         # "CQL_FILTER": f"{settings.wfs_date_field} BETWEEN '{start}' AND '{end}'",
#         "count": str(count),
#         "startIndex": str(start_index),
#         "sortBy": settings.wfs_sortby,
#     }
#     url = _wfs_url(params)
#     async with httpx.AsyncClient(timeout=60) as client:
#         r = await client.get(url)
#         r.raise_for_status()
#         return r.json()

# async def iter_wfs(
#     start: str,
#     end: str,
# ) -> AsyncIterator[dict]:
#     start_index = 0
#     while True:
#         data = await fetch_wfs_page(start, end, start_index=start_index)
#         feats = data.get("features", [])
#         if not feats:
#             break
#         for f in feats:
#             yield f
#         start_index += len(feats)
#         if len(feats) < settings.wfs_page_size:
#             break

@retry(
    reraise=True,
    stop=stop_after_attempt(settings.retry_max_attempts),
    wait=wait_exponential(multiplier=settings.retry_multiplier, max=settings.retry_max_wait),
    retry=retry_if_exception_type(httpx.HTTPError),
)
@wfs_breaker
async def _fetch(url: str) -> Dict[str, Any]:
    # chamada HTTP resiliente + circuit breaker
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.json()

async def fetch_wfs_page_48h(start_index: int = 0, count: int = settings.wfs_page_size) -> Dict[str, Any]:
    """
    Para a camada "48h" NÃO usamos CQL_FILTER por data (a camada já recorta).
    IMPORTANTE: muitos serviços exigem namespace no typeNames (ex.: dados_abertos:).
    Se o seu .env tiver só "focos_48h_br_satref" e der erro,
    troque para "dados_abertos:focos_48h_br_satref".
    """
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": settings.wfs_typename,   # ex.: "dados_abertos:focos_48h_br_satref"
        "srsName": settings.wfs_srid,         # "EPSG:4326"
        "outputFormat": "application/json",
        "count": str(count),
        "startIndex": str(start_index),
        # Em algumas instâncias o campo é "ID" (maiúsculo). Ajuste no .env (WFS_SORTBY).
        # "sortBy": settings.wfs_sortby,        # ex.: "ID"
    }
    # só inclui sortBy se estiver no .env e não for vazio
    if settings.wfs_sortby and settings.wfs_sortby.strip():
        params["sortBy"] = settings.wfs_sortby.strip()
    
    url = _wfs_url(params)
    log.info("wfs.request", url=url, start_index=start_index, count=count)
    
    
    try:
        data = await _fetch(url)
        feats = data.get("features", [])
        log.info("wfs.response", received=len(feats))
        return data
    except HTTPStatusError as e:
        # se for 400 e mandamos sortBy, tenta de novo sem sortBy (campo inválido)
        if e.response.status_code == 400 and "sortBy" in params:
            body = e.response.text[:300]
            log.warning("wfs.sortby_invalid_retrying",
                        sortBy=params["sortBy"],
                        status=e.response.status_code,
                        body_snippet=body)
            params.pop("sortBy", None)
            url2 = _wfs_url(params)
            data = await _fetch(url2)
            feats = data.get("features", [])
            log.info("wfs.response_no_sort", received=len(feats))
            return data
        raise

async def fetch_wfs_page_48h_v2(start_index: int = 0, count: int = settings.wfs_page_size) -> dict:
    """
    Busca uma página da camada 48h, tentando combinações robustas caso o servidor
    rejeite algum parâmetro (outputFormat, count/startIndex, typeNames/typeName, versão).
    """
    sortby = _clean_sortby(settings.wfs_sortby)
    # Variações de outputFormat comumente aceitas por GeoServer
    output_formats = [
        "application/json",
        "json",
        "geojson",
        "application/json; subtype=geojson",
        "application/geo+json",
    ]

    attempts: list[dict] = []

    # ----- WFS 2.0.0 -----
    for of in output_formats:
        base_params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": settings.wfs_typename,
            "srsName": settings.wfs_srid,
            "outputFormat": of,
        }
        # com paginação 2.0
        p = base_params.copy()
        p["count"] = str(count)
        p["startIndex"] = str(start_index)
        if sortby:
            p["sortBy"] = sortby
        attempts.append(p)

        # sem sortBy
        if sortby:
            p2 = p.copy(); p2.pop("sortBy", None); attempts.append(p2)

        # sem paginação (alguns servidores dão 400 com count/startIndex)
        attempts.append({k: v for k, v in base_params.items()})

    # ----- WFS 1.1.0 (alguns servers preferem) -----
    for of in output_formats:
        base_params = {
            "service": "WFS",
            "version": "1.1.0",
            "request": "GetFeature",
            "typeName": settings.wfs_typename,  # note: sem 's' aqui
            "srsName": settings.wfs_srid,
            "outputFormat": of,
        }
        # “estilo antigo” de paginação: maxFeatures + startIndex (extensão)
        p = base_params.copy()
        p["maxFeatures"] = str(count)
        p["startIndex"] = str(start_index)
        if sortby:
            p["sortBy"] = sortby
        attempts.append(p)

        # sem sortBy
        if sortby:
            p2 = p.copy(); p2.pop("sortBy", None); attempts.append(p2)

        # sem paginação
        attempts.append({k: v for k, v in base_params.items()})

    # ----- fallback “mínimo” (sem outputFormat => GML default) -----
    attempts.append({
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": settings.wfs_typename,
        "srsName": settings.wfs_srid,
    })

    # tenta na ordem
    last_err_snippet = None
    for params in attempts:
        url = _wfs_url(params)
        try:
            log.info("wfs.request", url=url, start_index=start_index, count=count)
            data = await _fetch(url)  # espera JSON quando pedimos JSON
            feats = data.get("features", [])
            log.info("wfs.response", received=len(feats), of=params.get("outputFormat"))
            return data
        except HTTPStatusError as e:
            body = e.response.text if e.response is not None else ""
            last_err_snippet = body[:400]
            log.warning("wfs.attempt_failed",
                        status=e.response.status_code if e.response else None,
                        params=params, body_snippet=last_err_snippet)
            continue
        except ValueError as e:
            # se cair aqui provavelmente veio GML (não-JSON) no fallback mínimo
            txt = str(e)
            log.warning("wfs.non_json_response", error=txt, params=params)
            # não retorna; tenta próximo
            continue

    # se nenhuma combinação funcionar, propaga erro com contexto
    raise RuntimeError(f"WFS GetFeature falhou em todas as combinações. Última pista: {last_err_snippet!r}")

async def iter_wfs_48h() -> AsyncIterator[Dict[str, Any]]:
    """
    Itera todas as páginas da camada 48h com paginação WFS 2.0.0 (count/startIndex/sortBy).
    """
    start_index = 0
    while True:
        data = await fetch_wfs_page_48h(start_index=start_index)
        feats = data.get("features", [])
        if not feats:
            break
        for f in feats:
            yield f
        start_index += len(feats)
        if len(feats) < settings.wfs_page_size:
            break

# (opcional) versão com filtro por data para outras camadas (ex.: mês atual)
async def fetch_wfs_page_by_date(start: str, end: str, start_index: int = 0, count: int = settings.wfs_page_size) -> Dict[str, Any]:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": settings.wfs_typename,
        "srsName": settings.wfs_srid,
        "outputFormat": "application/json",
        "CQL_FILTER": f"{settings.wfs_date_field} BETWEEN '{start}' AND '{end}'",
        "count": str(count),
        "startIndex": str(start_index),
        "sortBy": settings.wfs_sortby,
    }
    url = _wfs_url(params)
    log.info("wfs.request", url=url, start=start, end=end, start_index=start_index, count=count)
    data = await _fetch(url)
    feats = data.get("features", [])
    log.info("wfs.response", received=len(feats))
    return data