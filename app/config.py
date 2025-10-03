
# app/config.py
from pydantic import BaseModel, Field, validator
import os
from pathlib import Path
from dotenv import load_dotenv

def _load_layered_env():
    """
    Carrega .env em camadas, nesta ordem:
      1) .env (base, sem segredos)
      2) .env.<APP_ENV>
      3) .env.local           (segredos locais; não commitar)
      4) .env.<APP_ENV>.local (mais específico)
    A cada etapa usamos override=True para sobrepor as anteriores.
    """
    cwd = Path.cwd()
    base = cwd / ".env"
    # primeira passada: base
    if base.exists():
        load_dotenv(base, override=False)

    app_env = os.getenv("APP_ENV", "local")
    env_file = cwd / f".env.{app_env}"
    if env_file.exists():
        load_dotenv(env_file, override=True)

    local_file = cwd / ".env.local"
    if local_file.exists():
        load_dotenv(local_file, override=True)

    env_local = cwd / f".env.{app_env}.local"
    if env_local.exists():
        load_dotenv(env_local, override=True)

# executa o carregamento em camadas
_load_layered_env()

class Settings(BaseModel):
    """
    Configurações da aplicação (carregadas de variáveis de ambiente).
    Use .env base (sem segredos) + arquivos específicos por ambiente
    (.env.local, .env.dev, .env.prod etc.) para segredos como MONGODB_URI.
    """
    # --- App / env ---
    app_env: str = Field(default=os.getenv("APP_ENV", "local"))
    log_level: str = Field(default=os.getenv("LOG_LEVEL", "INFO"))

    # --- MongoDB ---
    mongodb_uri: str | None = Field(default=os.getenv("MONGODB_URI"))
    mongodb_db: str = Field(default=os.getenv("MONGODB_DB", "inpe_db"))
    mongodb_coll: str = Field(default=os.getenv("MONGODB_COLLECTION", "focos_48h")) # "focos"
    
    # --- WFS / BDQueimadas ---
    wfs_base: str = Field(default=os.getenv("WFS_BASE", "https://terrabrasilis.dpi.inpe.br/queimadas/geoserver"))
    wfs_service_path: str = Field(default=os.getenv("WFS_SERVICE_PATH", "/wfs")) # "WFS_SERVICE_PATH", "/deter-amz/wfs"
    wfs_typename: str = Field(default=os.getenv("WFS_TYPENAME", "dados_abertos:focos_48h_br_satref")) # "WFS_TYPENAME", "deter_public"
    wfs_date_field: str = Field(default=os.getenv("WFS_DATE_FIELD", "data_hora_gmt")) # "WFS_DATE_FIELD", "date"
    wfs_srid: str = Field(default=os.getenv("WFS_SRID", "EPSG:4326")) # "WFS_SRID", "EPSG:4674"
    wfs_page_size: int = Field(default=int(os.getenv("WFS_PAGE_SIZE", "1000")))
    wfs_sortby: str = Field(default=os.getenv("WFS_SORTBY", "data_hora_gmt")) # "WFS_SORTBY", "gid"

    # --- Janelas (quando usar ingestão por datas) ---
    initial_start: str = Field(default=os.getenv("INITIAL_START", "2019-01-01"))
    initial_end: str = Field(default=os.getenv("INITIAL_END", "2020-01-01"))

    # --- Scheduler ---
    schedule_cron: str = Field(default=os.getenv("SCHEDULE_CRON", "*/10 * * * *"))
    
    # --- Robustez: retries / breaker ---
    retry_max_attempts: int = Field(default=int(os.getenv("RETRY_MAX_ATTEMPTS", "6")))
    retry_multiplier: float = Field(default=float(os.getenv("RETRY_MULTIPLIER", "0.5")))
    retry_max_wait: float = Field(default=float(os.getenv("RETRY_MAX_WAIT", "30")))
    
    # --- pybreaker (circuit breaker) ---
    breaker_fail_max: int = Field(default=int(os.getenv("BREAKER_FAIL_MAX", "5")))
    breaker_reset_timeout: int = Field(default=int(os.getenv("BREAKER_RESET_TIMEOUT", "60")))
    
    # --- logging / retries / breaker ---
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    @validator("wfs_page_size")
    def _positive_pagesize(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("WFS_PAGE_SIZE deve ser > 0")
        return v

    def masked_mongodb_uri(self) -> str:
        """
        Mascara user:pass na URI para logs seguros.
        """
        if not self.mongodb_uri:
            return "<unset>"
        # substitui credenciais por ***
        return self.mongodb_uri.replace(self.mongodb_uri.split("://", 1)[-1].split("@")[0], "***")


settings = Settings()
