# app/main.py
from fastapi import FastAPI

from .api.v1.routers import api as api_v1



# from .api.v1.routers import data, health, ingest
# import uvicorn
# from fastapi import FastAPI, Request
# import uuid
# from structlog.contextvars import bind_contextvars, clear_contextvars

# from .routers_old import debug, schema, debug_data
# from .core.scheduler import start_scheduler, stop_scheduler
# from .core.logging_config import setup_logging, get_logger

# setup_logging()
# log = get_logger()

# def create_app() -> FastAPI:
#     app = FastAPI(title="INPE → Mongo Sync")
    
#     # middleware opcional para correlacionar requisições
#     @app.middleware("http")
#     async def add_request_id(request: Request, call_next):
#         request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
#         request.state.request_id = request_id
        
#         # liga o request_id neste "task context"
#         bind_contextvars(request_id=request_id)
        
#         # log.bind(request_id=request_id)
#         try:
#             response = await call_next(request)
#             response.headers["x-request-id"] = request_id
#             return response
#         finally:
#             # remove bind após resposta (evita "vazar" contexto)
#             # log.unbind("request_id")
            
#             # limpa SEM levantar KeyError
#             clear_contextvars()
    
#     app.include_router(health.router)
#     app.include_router(ingest.router)
#     app.include_router(schema.router)
#     app.include_router(debug.router)
#     app.include_router(data.router)
#     app.include_router(debug_data.router)
    
#     @app.on_event("startup")
#     async def _startup():
#         log.info("app.startup")
#         start_scheduler(app)

#     @app.on_event("shutdown")
#     async def _shutdown():
#         log.info("app.shutdown")
#         stop_scheduler(app)

#     return app

# app = create_app()

tags_metadata = [
    {"name": "Health", "description": "Service liveness/readiness."},
    {"name": "Ingestion", "description": "Ingest data from TerraBrasilis WFS (48h, etc)."},
    {"name": "Data", "description": "Query/Stats for stored focus documents."},
]

app = FastAPI(
    title="INPE Sync API",
    version="1.0.0",
    openapi_tags=tags_metadata,
    summary="Versioned API to ingest and query TerraBrasilis fire detections.",
)

app.include_router(api_v1, prefix="/api/v1")

# if __name__ == "__main__":
#     uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
