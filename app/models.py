# app/models.py
from typing import Optional, List, Literal, Dict, Any
from pydantic import BaseModel, Field, conint, confloat

class HealthResponse(BaseModel):
    ok: bool = True

class IngestResponse(BaseModel):
    status: str
    layer: Optional[str] = None
    total_upserted: Optional[int] = None
    range: Optional[list[str]] = None
    last_seen: Optional[str] = None

class WFSSchemaResponse(BaseModel):
    typeNames: str
    attr_count: int
    attributes: List[str]
    xsd_snippet: str | None = None

class WFSSampleResponse(BaseModel):
    layer: str
    received: int
    preview_ids: List[str]

# --- Saídas ---
class SatelliteCount(BaseModel):
    satelite: Optional[str] = Field(None, description="Nome do satélite")
    count: int = Field(..., ge=0)

class StatsResponse(BaseModel):
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
    
# --- Entradas (query) ---

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