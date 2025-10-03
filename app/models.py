from pydantic import BaseModel
from typing import List, Optional

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
