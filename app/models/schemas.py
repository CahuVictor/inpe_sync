# app/models/schemas.py
from __future__ import annotations
from typing import Optional, Dict, Any, List, Literal, Annotated
from pydantic import BaseModel, Field, field_validator, ConfigDict, conint, confloat

class HealthResponse(BaseModel):
    ok: bool = True

# ---------- Saídas ----------
class SatelliteCount(BaseModel):
    satelite: Optional[str] = Field(None, description="Nome do satélite")
    count: int = Field(..., ge=0)

class StatsResponse(BaseModel):
    model_config = {"json_schema_extra": {"example": {
        "total": 2172,
        "min_data_hora_gmt": "2025-10-03T00:00:00Z",
        "max_data_hora_gmt": "2025-10-04T23:59:59Z",
        "by_satelite": [{"satelite":"AQUA_M-T","count":1234}]
    }}}
    total: int
    min_data_hora_gmt: Optional[str] = None
    max_data_hora_gmt: Optional[str] = None
    by_satelite: List[SatelliteCount] = []

class FocusItem(BaseModel):
    """Documento simplificado de foco."""
    id: str
    data_hora_gmt: Optional[str] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    satelite: Optional[str] = None
    municipio: Optional[str] = None
    estado: Optional[str] = None
    pais: Optional[str] = None
    bioma: Optional[str] = None
    frp: Optional[float] = None
    geometry: Optional[Dict[str, Any]] = None

class FocusListResponse(BaseModel):
    total: int
    returned: int
    items: List[FocusItem]

class GeoJSONFeature(BaseModel):
    type: Literal["Feature"] = "Feature"
    id: Optional[str] = None
    geometry: Optional[Dict[str, Any]] = None
    properties: Dict[str, Any]

class GeoJSONFeatureCollection(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[GeoJSONFeature]

class IngestResponse(BaseModel):
    status: str
    layer: Optional[str] = None
    total_upserted: Optional[int] = None
    range: Optional[list[str]] = None
    last_seen: Optional[str] = None
    duration_ms: Optional[int] = None
    
    model_config = {
        "json_schema_extra": {
            "example": {
                "status": "ok",
                "layer": "dados_abertos:focos_48h_br_satref",
                "total_upserted": 2172,
                "range": ["2025-10-01", "2025-10-03"],
                "last_seen": "2025-10-03T17:09:00Z",
                "duration_ms": 1234,
            }
        }
    }

# ---------- Entradas (query) ----------
class QueryParams(BaseModel):
    """Parâmetros de busca textual / temporal / espacial."""
    start: Optional[str] = Field(None, description="Data inicial (YYYY-MM-DD)")
    end: Optional[str] = Field(None, description="Data final (YYYY-MM-DD)")
    satelite: Optional[str] = None
    estado: Optional[str] = None
    municipio: Optional[str] = None
    bioma: Optional[str] = None

    # proximidade (raio) – usa índice 2dsphere
    near_lon: Optional[confloat(ge=-180, le=180)] = None
    near_lat: Optional[confloat(ge=-90, le=90)] = None
    near_km: Optional[confloat(gt=0)] = None

    # bbox: minLon,minLat,maxLon,maxLat
    bbox: Optional[str] = Field(
        None,
        description="minLon,minLat,maxLon,maxLat"
    )

    # paginação/ordenação
    limit: conint(gt=0, le=1000) = 100
    skip: conint(ge=0) = 0
    sort: Literal["-data_hora_gmt", "data_hora_gmt"] = "-data_hora_gmt"

    # formato
    format: Literal["json", "geojson"] = "json"

    @field_validator("end")
    @classmethod
    def _end_after_start(cls, v: Optional[str], info):
        start = info.data.get("start")
        if start and v and v < start:
            raise ValueError("end must be >= start")
        return v

    @field_validator("bbox")
    @classmethod
    def _bbox_fmt(cls, v: Optional[str]):
        if not v:
            return v
        parts = v.split(",")
        if len(parts) != 4:
            raise ValueError("bbox must be minLon,minLat,maxLon,maxLat")
        return v

class WFSSchemaResponse(BaseModel):
    typeNames: str
    attr_count: int
    attributes: List[str]
    xsd_snippet: str | None = None

# class WFSSampleResponse(BaseModel):
#     layer: str
#     received: int
#     preview_ids: List[str]