# app/logging_config.py
import logging
import os
import structlog
from structlog.contextvars import merge_contextvars

def setup_logging():
    """
    Inicializa logging + structlog em JSON.

    - Usa LOG_LEVEL (default INFO).
    - Habilita processadores do structlog (timestamp, stack info, exc info).
    - `merge_contextvars` injeta contextvars (ex.: request_id) nos logs.
    """
    # nível via env (INFO padrão)
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    # integra logging padrão do Python com structlog
    logging.basicConfig(
        level=log_level,
        format="%(message)s",
    )

    structlog.configure(
        processors=[
            merge_contextvars,                      # <- junta contextvars no evento
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),   # saída JSON
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level, logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

def get_logger():
    """
    Retorna um logger do structlog já configurado.
    """
    return structlog.get_logger()
