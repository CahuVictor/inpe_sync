# app/services/wfs_service.py
from __future__ import annotations
from typing import AsyncIterator, Dict, Any, Optional
from urllib.parse import urlencode, quote_plus, quote
import httpx
# from pybreaker import CircuitBreaker   # alterar para aiobreaker
import pybreaker
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential, retry_if_exception_type, wait_exponential_jitter

from .protocols import FireSource
from ..core.config import settings
from ..core.logging_config import get_logger

log = get_logger()

breaker = pybreaker.CircuitBreaker(    # pybreaker.CircuitBreaker
    fail_max=settings.breaker_fail_max,
    reset_timeout=settings.breaker_reset_timeout,   # antes reset_timeout recovery_timeout
    # timeout=float(settings.breaker_reset_timeout),  # segundos
    # name="wfs-http",
)

def _norm_iso(day_or_iso: str, *, end: bool = False) -> str:
    """Aceita 'YYYY-MM-DD' ou ISO completo; completa hora se vier só a data."""
    if "T" in day_or_iso:
        return day_or_iso
    return f"{day_or_iso}T{'23:59:59Z' if end else '00:00:00Z'}"

class WfsFireSource(FireSource):
    """
    Implementa coleta no GeoServer TerraBrasilis (WFS 2.0).
    Expõe:
      - iter_48h(): usa a camada já recortada de 48h (sem CQL)
      - iter_range(start, end): usa CQL_FILTER por data
    """
    # def __init__(self, client: httpx.AsyncClient | None = None) -> None:
    #     self._client = client or httpx.AsyncClient(timeout=60)
    # def __init__(self) -> None:
    def __init__(
        self,
        base: str | None = None,
        service_path: str | None = None,
        typename_48h: str | None = None,
        typename_hist: Optional[str] = None,
        date_field: str | None = None,
        page_size: int | None = None,
        sortby: str | None = None,
    ) -> None:
        # self.base = settings.wfs_base.rstrip("/")
        # self.path = settings.wfs_service_path
        # self.type_name = settings.wfs_typename
        # self.date_field = settings.wfs_date_field
        # self.page_size = settings.wfs_page_size
        # self.sortby = settings.wfs_sortby
        # self.srid = settings.wfs_srid
        # self._client = httpx.AsyncClient(timeout=30)
        
        self.base = base or settings.wfs_base
        self.service_path = service_path or settings.wfs_service_path
        self.typename_48h = typename_48h or settings.wfs_typename
        self.typename_hist = typename_hist or settings.wfs_typename_hist  # pode ser None
        self.date_field = date_field or settings.wfs_date_field
        self.page_size = page_size or settings.wfs_page_size
        self.sortby = sortby or settings.wfs_sortby
        self._client = httpx.AsyncClient(timeout=30.0)
    
    def _url(self, *, start_index: int = 0, cql: Optional[str] = None) -> str:
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": self.type_name,
            "srsName": self.srid,
            "outputFormat": "application/json",
            "count": str(self.page_size),
            "startIndex": str(start_index),
            "sortBy": self.sortby,
        }
        if cql:
            params["cql_filter"] = cql
        return f"{self.base}{self.path}?{urlencode(params, quote_via=quote_plus)}"

    async def _get_json(self, url: str) -> Dict[str, Any]:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(settings.retry_max_attempts),
            wait=wait_exponential(
                multiplier=settings.retry_multiplier,
                max=settings.retry_max_wait
            ),
            # retry=retry_if_exception_type((httpx.HTTPError,)),
            reraise=True,
        ):
            with attempt:
                r = await breaker.call_async(self._client.get, url)      # .call(self._client.get, url, timeout=60)
                r.raise_for_status()
                return r.json()
    
    def _base_params(self, typename: str) -> Dict[str, Any]:
        return dict(
            service="WFS",
            version="2.0.0",
            request="GetFeature",
            typeNames=typename,
            srsName=settings.wfs_srid,
            outputFormat="application/json",
            count=self.page_size,
            sortBy=self.date_field,
        )
    
    async def _paginate(self, *, cql: Optional[str]) -> AsyncIterator[Dict[str, Any]]:
        start_index = 0
        total = 0
        while True:
            url = self._url(start_index=start_index, cql=cql)
            data = await self._get_json(url)
            feats = data.get("features") or []
            got = len(feats)
            log.info("wfs.response", received=got, start_index=start_index)
            if not feats:
                break
            for f in feats:
                yield f
            total += got
            if got < self.page_size:
                break
            start_index += self.page_size
        log.info("wfs.done", total=total)

    # -------- APIs públicas --------
    
    # async def iter_48h(self, page_size: int = 1000) -> AsyncIterator[Dict[str, Any]]:
    #     base = f"{settings.wfs_base}{settings.wfs_service_path}"
    #     typename = settings.wfs_typename
    #     start = 0
    #     while True:
    #         url = (
    #             f"{base}?service=WFS&version=2.0.0&request=GetFeature"
    #             f"&typeNames={typename}&srsName={settings.wfs_srid}"
    #             f"&outputFormat=application/json&count={page_size}&startIndex={start}"
    #             f"&sortBy={settings.wfs_sortby}"
    #         )
    #         data = await self._get_json(url)
    #         feats = data.get("features", [])
    #         if not feats:
    #             break
    #         for f in feats:
    #             yield f
    #         start += len(feats)
    # async def iter_48h(self) -> AsyncIterator[Dict[str, Any]]:
    #     """Para a camada '48h' (já filtrada no servidor)."""
    #     async for feat in self._paginate(cql=None):
    #         yield feat
    async def iter_48h(self) -> AsyncIterator[Dict[str, Any]]:
        typename = self.typename_48h
        start = 0
        total = 0
        while True:
            params = self._base_params(typename)
            params["startIndex"] = start
            url = f"{self.base}{self.service_path}?{urlencode(params, safe=':,')}"
            data = await self._get_json(url)
            feats = (data or {}).get("features") or []
            log.info("wfs.response", start_index=start, received=len(feats))
            if not feats:
                break
            for f in feats:
                yield f
                total += 1
            if len(feats) < self.page_size:
                break
            start += self.page_size
        log.info("wfs.done", total=total)
    
    # async def iter_range(self, start: str, end: str) -> AsyncIterator[Dict[str, Any]]:
    #     """
    #     Varre o intervalo [start, end] (inclusive) usando CQL sobre o campo configurado.
    #     Aceita 'YYYY-MM-DD' ou ISO; completa hora ao início/fim do dia.
    #     """
    #     cql = (
    #         f"{self.date_field} >= '{_norm_iso(start)}' AND "
    #         f"{self.date_field} <= '{_norm_iso(end, end=True)}'"
    #     )
    #     log.info("wfs.request.range", start=start, end=end, field=self.date_field)
    #     async for feat in self._paginate(cql=cql):
    #         yield feat
    async def iter_range(self, start_date: str, end_date: str, typename: Optional[str] = None) -> AsyncIterator[Dict[str, Any]]:
        """
        Faz paginação por intervalo arbitrário via CQL_FILTER:
          data_hora_gmt BETWEEN startT00:00:00Z AND endT23:59:59Z
        Usa typename histórico se disponível, senão cai no typename_48h.
        """
        chosen_typename = typename or self.typename_hist or self.typename_48h
        start_idx = 0
        total = 0

        # CQL para o campo de data configurado
        cql = f"{self.date_field} BETWEEN {start_date}T00:00:00Z AND {end_date}T23:59:59Z"

        while True:
            params = self._base_params(chosen_typename)
            params["startIndex"] = start_idx
            # geoserver aceita `cql_filter` (minúsculo). Evite encoding dos espaços/operadores:
            params["cql_filter"] = cql

            # urlencode normal funciona; se quiser insistir em legibilidade: safe=' :,<>=T'
            url = f"{self.base}{self.service_path}?{urlencode(params, safe=' :,<>=T')}"

            log.info("wfs.request.range", field=self.date_field, start=start_date, end=end_date)
            data = await self._get_json(url)
            feats = (data or {}).get("features") or []
            log.info("wfs.response", start_index=start_idx, received=len(feats))
            if not feats:
                break
            for f in feats:
                yield f
                total += 1
            if len(feats) < self.page_size:
                break
            start_idx += self.page_size

        log.info("wfs.done", total=total)
