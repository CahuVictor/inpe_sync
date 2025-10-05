# INPE → Mongo Sync (BDQueimadas · 48h)
**FastAPI · Poetry · httpx · Motor (MongoDB Atlas) · APScheduler · structlog · tenacity · pybreaker**

Sincroniza **focos de queimadas (últimas 48h)** do **BDQueimadas/INPE** (GeoServer/WFS TerraBrasilis) para **MongoDB Atlas**, com:
- **Upsert idempotente** (chave única por foco)
- **Retries exponenciais** (tenacity)
- **Circuit breaker** (pybreaker)
- **Logs estruturados** (structlog + request_id)
- **Agendador interno** (APScheduler)
- Endpoints para **depuração** do WFS e **consulta** no Mongo
- **Ambientes .env em camadas** (base + específico + local)

> **Status**: estável para ingestão 48h e consultas básicas; incremental disponível via scheduler.

> Esta versão incorpora correções confirmadas em execução: **layer, campo de data e sort**, _fallbacks_ para WFS, middleware de **request_id** com `contextvars`, **endpoints de debug** e dicas para PowerShell.

---

## ✅ O que foi corrigido/validado

- **Camada WFS**: `dados_abertos:focos_48h_br_satref` (instância Queimadas).  
- **Campo de data**: `data_hora_gmt` (datetime).  
- **Ordenação**: `sortBy=data_hora_gmt` (funciona nessa camada).  
- **Cliente WFS com fallbacks**:
  - Tenta `WFS 2.0.0` e `1.1.0`, `typeNames` x `typeName`,
  - variações de `outputFormat` (`application/json`, `json`, `geojson`, …),
  - e desativa `sortBy` automaticamente quando o servidor rejeita o campo.
- **Logs estruturados** (`structlog`) com **`request_id`** via `contextvars`.  
- **Endpoints de depuração**: `/debug/wfs-schema` e `/debug/wfs-sample` (operam sem Mongo).  
- **Mapeamento de campos** (48h): chave única por `foco_id` (ou `id_foco_bdq`), data em `data_hora_gmt`, geometrias GeoJSON com índice `2dsphere`.  
- **PowerShell**: instruções para testar URLs com **`curl.exe`** (evita erro com `&`).

---

## Sumário
- [Arquitetura](#arquitetura)
- [Requisitos e instalação](#requisitos-e-instalação)
- [Configuração (.env em camadas)](#configuração-env-em-camadas)
- [Variáveis principais](#variáveis-principais)
- [Execução](#execução)
- [Endpoints](#endpoints)
- [Modelo de dados & índices](#modelo-de-dados--índices)
- [Logs, retries e circuit breaker](#logs-retries-e-circuit-breaker)
- [Mudanças recentes e justificativas](#mudanças-recentes-e-justificativas)
- [Troubleshooting](#troubleshooting)
- [Estrutura do projeto](#estrutura-do-projeto)
- [Licença e créditos](#licença-e-créditos)

---

## Arquitetura
- **FastAPI** expõe rotas de ingestão, depuração e leitura.
- **httpx** consome o WFS com **fallbacks** (WFS 2.0.0/1.1.0, `typeNames`/`typeName`, formatos e `sortBy` opcional).
- **Motor** (async) grava no **MongoDB Atlas** com **bulk upsert**.
- **APScheduler** roda sincronização incremental/48h no cron configurável.
- **structlog** formata logs em JSON com **request_id** via `contextvars`.
- **tenacity** + **pybreaker** trazem resiliência (backoff + circuit breaker).

---

## Requisitos e instalação
- **Python** 3.11+
- **Poetry** (gerenciador de dependências)

### 1) Por que Poetry?
O projeto é empacotado com **Poetry**, que gerencia dependências e ambiente virtual automaticamente.

### Instalação
```bash
poetry install
```

> Para conexões **SRV** do Atlas, o `motor/pymongo` já inclui `dnspython` como dependência. Em setups minimalistas, se precisar:
>
> ```bash
> poetry add dnspython
> ```

---

## Configuração (.env em camadas)
O projeto carrega variáveis em **camadas**, nesta ordem:
1. `.env` (base, sem segredos)
2. `.env.<APP_ENV>`
3. `.env.local` (segredos locais; **não** versionar)
4. `.env.<APP_ENV>.local` (segredos específicos por ambiente; **não** versionar)

Exemplo mínimo de **`.env`** (base):

```dotenv
APP_ENV=local
LOG_LEVEL=INFO

# MongoDB Atlas
MONGODB_URI=mongodb+srv://<user>:<pass>@<cluster>/?retryWrites=true&w=majority
MONGODB_DB=inpe_db
MONGODB_COLLECTION=focos

# ===== TerraBrasilis / BDQueimadas (WFS) =====
WFS_BASE=https://terrabrasilis.dpi.inpe.br/queimadas/geoserver
WFS_SERVICE_PATH=/wfs                                  # caminho do workspace/serviço
WFS_TYPENAME=dados_abertos:focos_48h_br_satref         # nome exato da camada (feature type)

# Campo de data e sort confirmados no DescribeFeatureType
WFS_DATE_FIELD=data_hora_gmt                           # atributo de data (para filtros incrementais)
WFS_SORTBY=data_hora_gmt                               # campo único para paginação estável

# CRS e paginação
WFS_SRID=EPSG:4326                                     # CRS do resultado
WFS_PAGE_SIZE=1000                                     # paginação

# Janela inicial (ingestão “full” inicial ou fatia histórica)
INITIAL_START=2019-01-01
INITIAL_END=2020-01-01

# Agendamento (cron): a cada 30 minutos
SCHEDULE_CRON=*/30 * * * *

# Logs / Robustez
LOG_LEVEL=INFO
RETRY_MAX_ATTEMPTS=6
RETRY_MULTIPLIER=0.5
RETRY_MAX_WAIT=30
BREAKER_FAIL_MAX=5
BREAKER_RESET_TIMEOUT=60
```

Exemplo de **`.env.local`** (segredos do Atlas – **não** comitar):
```dotenv
MONGODB_URI=mongodb+srv://<user>:<pass>@<cluster>/?retryWrites=true&w=majority&appName=<app>
MONGODB_DB=inpe_db
MONGODB_COLLECTION=focos_48h
```

> **Dica** (PowerShell): use `curl.exe "URL COMPLETA COM &"` (entre aspas) para não quebrar a linha de comando.

> **Dica**: ajuste `WFS_SERVICE_PATH`, `WFS_TYPENAME` e `WFS_DATE_FIELD` conforme **a camada que você escolher** (DETER, PRODES, BDQueimadas, etc.). Veja o guia “Descobrir a camada” abaixo.

> **Dica**: Não comente na mesma linha (`WFS_SORTBY=# id`) — o `#` vira parte do valor! Comente em linha separada.

---

## Variáveis principais

| Variável | Padrão | Descrição |
|---|---:|---|
| `APP_ENV` | `local` | Seleciona perfil de .env em camadas. |
| `LOG_LEVEL` | `INFO` | Nível de log. |
| `MONGODB_URI` | — | URI Atlas (SRV). Defina em `.env.local`. |
| `MONGODB_DB` | `inpe_db` | Nome do BD. |
| `MONGODB_COLLECTION` | `focos_48h` | Coleção destino (BREAKING CHANGE vs versões antigas). |
| `WFS_*` | ver acima | Base, path, typeName, campo de data, CRS, paginação, sort. |
| `SCHEDULE_CRON` | `*/10 * * * *` | Cron do agendador. |
| `RETRY_MAX_ATTEMPTS` | `6` | Tenacity – máximo de tentativas. |
| `RETRY_MULTIPLIER` | `0.5` | Tenacity – base do backoff exponencial. |
| `RETRY_MAX_WAIT` | `30` | Tenacity – espera máxima entre tentativas (s). |
| `BREAKER_FAIL_MAX` | `5` | pybreaker – falhas até abrir o circuito. |
| `BREAKER_RESET_TIMEOUT` | `60` | pybreaker – tempo para semi-open (s). |

---

## Execução

```bash
poetry run uvicorn app.main:app --reload --port 8000
```

---

Preset para **BDQueimadas 48h** (ajuste credenciais do Atlas):

## Camada (typeName)
- `focos_48h_br_satref` — disponível no WFS do TerraBrasilis/Queimadas.
  - Confirmado no `GetCapabilities` do serviço WFS (Name/Title).
  - CRS: `EPSG:4326`.

---

## 3) Como **descobrir a camada** e os campos (typeName, data, etc.)

O TerraBrasilis roda **GeoServer** e expõe WFS/WMS. Você encontra:
- **`GetCapabilities` (WFS)**: lista os **feature types** (`typeName`) disponíveis.
- **`DescribeFeatureType`**: retorna o **schema** (atributos e tipos) de um `typeName` específico.
- **`GetFeature`**: baixa os dados (GeoJSON, GML, etc.).

### 3.1. `GetCapabilities`
Abra no navegador (exemplo BDQueimadas):
```
https://terrabrasilis.dpi.inpe.br/queimadas/geoserver/wfs?request=GetCapabilities
```
Busque pelo nome da camada (ex.: `focos_*`, `deter_*`, etc.) e anote o **`Name`** completo (geralmente `workspace:layer`).

### 3.2. `DescribeFeatureType`
Com o `typeName` anotado, descubra os atributos (incluindo o **campo de data**):
```
https://terrabrasilis.dpi.inpe.br/queimadas/geoserver/wfs?service=WFS&version=2.0.0&request=DescribeFeatureType&typenames=<workspace:layer>
```
Procure pelo atributo de data (ex.: `date`, `data`, `datetime`, `acq_date`…) e por uma **chave única** (ex.: `gid`, `id`).

### 3.3. `GetFeature` (GeoJSON) com **paginação** e **filtro por data**
```
https://<host>/<service_path>?service=WFS&version=2.0.0&request=GetFeature
&typeName=<workspace:layer>&outputFormat=application/json&srsName=EPSG:4674
&CQL_FILTER=<DATE_FIELD> BETWEEN '2025-01-01' AND '2025-01-07'
&count=1000&startIndex=0&sortBy=<unique_field>
```
- `count` = tamanho da página  
- `startIndex` = deslocamento (0, 1000, 2000, …)  
- `sortBy` = **campo único** (ex.: `gid`) para paginação estável  
- `CQL_FILTER` = filtro por intervalo de data

> **Nota**: alguns serviços usam `typeName` (1.x) vs `typenames` (2.0.0). Para 2.0.0, use `typenames` ou `typeName` conforme o servidor aceitar.

## Exemplo de teste (GetFeature, 48h completa — sem filtro de data)
```
https://terrabrasilis.dpi.inpe.br/queimadas/geoserver/wfs
  ?service=WFS&version=2.0.0&request=GetFeature
  &typeNames=focos_48h_br_satref
  &outputFormat=application/json
  &srsName=EPSG:4326
  &count=1000&startIndex=0
  &sortBy=id
```

---

## 4) Endpoints da API

**Ingestão**
- `POST /ingest/48h?dry_run=false&mock_write=false` — sincroniza as 48h (paginado, upsert idempotente). \
  > Parâmetros úteis:
  > - `dry_run=true` → não executa `bulk_write`, apenas baixa e mapeia.
  > - `mock_write=true` → injeta `_mock_upsert_many` (grava apenas uma **sonda** `__mock_48h__` para validar contagens sem impactar dados).
- `POST /ingest/initial` — carrega intervalo `INITIAL_START..INITIAL_END`.
- `POST /ingest/incremental?days=7` — janela relativa (para camadas com campo de data).

**Depuração WFS (não requer Mongo)**
- `GET /debug/wfs-schema` — `DescribeFeatureType` e lista de atributos.
- `GET /debug/wfs-sample?limit=10` — amostra de features direto do WFS.

**Consulta no Mongo**
- `GET /data/recent?limit=20&format=json|geojson` — últimos N focos.
- `GET /data/find?start=YYYY-MM-DD&end=YYYY-MM-DD&limit=100&skip=0&sort=-data_hora_gmt&format=json|geojson` — filtros textuais/temporais/espaciais (bbox/near).
- `GET /data/stats` — total, min/max `data_hora_gmt`, agregados por `satelite`.

**Debug de escrita**
- `POST /data/debug/write-test` — insere/atualiza um documento de teste (sanity check de conexão/índices). \
  > Pode estar em `routers/debug_data.py` (organização de rotas de diagnóstico).

**Saúde**
- `GET /health/health` — status básico.

**Docs**
- `GET /docs` — Swagger UI.



O **agendador** dispara `incremental` conforme `SCHEDULE_CRON`.

> Reprocessar janelas sobrepostas é **idempotente** porque usamos **upsert por `_id`** (campo `id` dos focos).

### Fluxo de ingestão (48h)
1) Cliente WFS busca páginas com `count/startIndex` (2.0.0), `outputFormat=application/json`, `sortBy=data_hora_gmt`.  
2) Se o servidor rejeitar algo (400): **fallback** para combinações alternativas.  
3) Cada `feature` vira `doc` com `_id = foco_id` (ou `id_foco_bdq`), e é **upsertado** (idempotente).

---

## Modelo de dados & índices

**Mapeamento (48h)**
- Chave: `id = foco_id` (fallback `id_foco_bdq`), espelhada em `_id`.
- Data/ordenação: `data_hora_gmt`.
- Geometria: `geometry` (GeoJSON), CRS `EPSG:4326`.
- `properties`: objeto completo retornado pelo WFS.

**Índices (criados na primeira conexão)**
- Único em **`id`** (evita duplicatas; viabiliza upsert).
- **`2dsphere`** em `geometry` (consultas espaciais).
- Ascendente em **`data_hora_gmt`** (ordenar/filtrar por tempo).

**Upsert em lote (Motor/PyMongo)** – uso de **`UpdateOne`**:
```python
from pymongo import UpdateOne

ops = []
for d in docs:
    _id = d["_id"]
    ops.append(UpdateOne({"_id": _id}, {"$set": d}, upsert=True))

if ops:
    res = await coll.bulk_write(ops, ordered=False)
```
> Isso evita o erro `TypeError: {...} is not a valid request` observado quando se passa dicts cru para `bulk_write`.

---

### 5) **`ingest_48h` com `dry_run` e `mock_write`**
```python
from fastapi import Query
from datetime import datetime

async def ingest_48h(dry_run: bool = Query(False), mock_write: bool = Query(False)):
    db, coll = await get_mongo()

    # opção de mock: substitui upsert_many só dentro desta requisição
    async def _mock_upsert_many(c, docs):
        # grava só uma sonda e conta quantos passaram
        await c.update_one(
            {"_id": "__mock_48h__"},
            {"$inc": {"calls": 1, "docs_seen": len(list(docs))},
             "$set": {"ts": datetime.utcnow().isoformat()}},
            upsert=True,
        )
        log.info("MOCK.upsert_many", docs_seen=len(list(docs)))

    upsert_fn = _mock_upsert_many if mock_write else real_upsert_many
    ...
```
**Por que?** Permite **validar toda a cadeia** (download → parse → map) **sem** gravar documentos reais. Útil para isolar problemas (ex.: permissão/índice) sem poluir dados.

---

### 6) **Scheduler: função direta (sem `lambda`)**
- **Antes**: `scheduler.add_job(lambda: run_incremental(), trig, name="incremental-sync")`
- **Agora**:  `scheduler.add_job(run_incremental, trig, name="incremental-sync")`

**Por que?** Evita coroutines **não aguardadas** (`coroutine was never awaited`) e segue o padrão correto do APScheduler para jobs **async** (o scheduler detecta e aguarda a coroutine).

---

## 5) Estratégia contra duplicidades e reprocessos

A coleção cria:
- índice **único** em `gid` (ou no campo escolhido) e espelha essa chave em **`_id`**;
- índice por **data** para queries de janela;

O **upsert** é feito com `bulk_write` (`updateOne + upsert: true`). Assim, **reprisar janelas sobrepostas** (ex.: `01–07` e depois `04–10`) é **idempotente**: registros repetidos **não** são inseridos novamente — apenas atualizados se mudarem.

### E se a fonte corrigiu dados antigos?
Ótimo: ao reprocessar a janela que contém o registro corrigido, o upsert sobrescreve o documento mantendo-o atualizado.

### Aprimoramentos úteis
- Adicionar `last_synced_at` (timestamp ingestão) e `src_etag`/`src_hash` (hash do feature) para detectar mudanças e pular updates quando o conteúdo é idêntico.
- Guardar `source_version`/`source_timestamp` se a camada fornecer (nem sempre existe).
- Opcional: **coleção de auditoria** com versões antigas (append-only) para trilha de mudanças.

---

## 6) Políticas de retenção (“sanitização”)

Opções combináveis:
1. **TTL** por data (útil para *staging*): índice TTL para expirar documentos antigos.
2. **Particionamento lógico**: coleções por ano (`focos_2025`, `focos_2026`).
3. **Arquivamento**: mover documentos pouco acessados para `focos_archive`.
4. **Janela ativa**: manter “N dias” mais recentes e (opcional) *pin* por áreas de interesse.

> Exemplo de TTL (atenção: só funciona com campos de data do tipo `Date`):
```python
await coll.create_index("expires_at", expireAfterSeconds=0)
```

---

## 7) Logs, **retries** e **circuit breaker** (robustez)

### Logs estruturados

- Config em `app/logging_config.py` (JSON).  
- Middleware em `app/main.py` usa `structlog.contextvars`:
  - `bind_contextvars(request_id=...)` no início da request;
  - `clear_contextvars()` no `finally` (sem `KeyError`).

Use `structlog` para JSON logs e correlação de chamadas:
```python
import structlog
log = structlog.get_logger()

log.info("wfs.fetch", start=start, end=end, start_index=start_index)
```

Exemplos que você verá:
```json
{"event":"wfs.request","url":"...GetFeature...","start_index":0,"count":10}
{"event":"wfs.response","received":10,"of":"application/json"}
{"event":"mongo.bulk_upsert","matched":100,"upserted":900,"modified":50}
```

### Retries exponenciais
- Simples com **tenacity**:
```python
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=0.5, max=20))
async def fetch_page(...):
    ...
```
- Ou backoff manual com `asyncio.sleep()` e jitter.

### Circuit breaker
Protege contra flutuações prolongadas da fonte:
```python
import pybreaker

wfs_breaker = pybreaker.CircuitBreaker(fail_max=5, reset_timeout=60)

@wfs_breaker
async def robust_fetch(...):
    return await fetch_page(...)
```
**Benefícios**: evita “tempestade de retries”, dá tempo para o serviço do INPE se recuperar e seu app continua responsivo.

---

## 8) Geo: `geometry` + índice `2dsphere`

Os dados vêm em **GeoJSON** (`geometry` + `properties`). Crie um índice `2dsphere` para consultas espaciais:
```python
await coll.create_index([("geometry", "2dsphere")])
```
Exemplos de query:
- **Por raio** (pontos próximos):
```python
docs = coll.find({
  "geometry": {
    "$near": {
      "$geometry": {"type":"Point","coordinates":[-35.2,-8.0]},
      "$maxDistance": 25_000
    }
  }
})
```
- **Dentro de polígono** (ex.: município/UC):
```python
docs = coll.find({
  "geometry": {
    "$geoWithin": {
      "$geometry": { "type":"Polygon", "coordinates":[ ... ] }
    }
  }
})
```

> Garanta `srsName=EPSG:4326` ou `4674` (lat/lon) no WFS e preserve o **GeoJSON** como vem. Evite reprojetar desnecessariamente.

---

## Mudanças recentes e justificativas

### 1) **Conexão Mongo (deps.py) com `server_api=ServerApi('1')`**
```python
from pymongo.server_api import ServerApi
from motor.motor_asyncio import AsyncIOMotorClient

_mongo_client = AsyncIOMotorClient(
    settings.mongodb_uri,
    server_api=ServerApi('1'),     # Passe ServerApi('1') como no snippet do Atlas - Se esse ping falhar, você verá o erro de auth na linha do ping (fica bem mais direto para diagnosticar).
    serverSelectionTimeoutMS=10000,
)
```
**Por que?** Mantém a **versão da API do servidor** estável (recomendação do Atlas) e ajuda a **evidenciar erros de autenticação/configuração** de forma direta, exatamente como nos exemplos oficiais do MongoDB. Facilita diagnóstico em ambientes novos.

---

### 2) **Models (Pydantic) para entradas/saídas**
```python
from typing import Optional, List, Dict, Any, Literal
from pydantic import BaseModel, Field, confloat, conint

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
```
**Por que?** Tipagem explícita melhora documentação (OpenAPI), validação e **consistência das respostas**. Facilita clientes externos e evita retornos irregulares.

---

### 3) **`upsert_many` com `UpdateOne` (bulk_write)**

**Antes** (causava `TypeError: ... is not a valid request` em alguns cenários):
```python
ops.append(
    {
        "updateOne": {
            "filter": {"_id": d["_id"]},
            "update": {"$set": d},
            "upsert": True,
        }
    }
)
```

**Agora**:
```python
from pymongo import UpdateOne

ops.append(UpdateOne({"_id": _id}, {"$set": d}, upsert=True))
```
**Por que?** O `bulk_write` do PyMongo/Motor espera **objetos de operação** (`UpdateOne`, `InsertOne` etc.). Isso evita erros de tipo e melhora a legibilidade/intellisense.

---

## Cliente WFS resiliente (tenacity + pybreaker)

- Retries exponenciais (`tenacity`) envolvendo a chamada HTTP.  
- Circuit breaker (`pybreaker`) para evitar tempestades de retries quando o WFS oscila.  
- Fallbacks automáticos de **versão**, **parâmetros** e **formatos** até obter resposta válida.

> Se o servidor ainda retornar 400, o log mostra um `body_snippet` com a razão (ex.: “Illegal property name”).

---

## Mapeamento de dados (48h)

- **Chave única**: `foco_id` (string). Se ausente, usar `id_foco_bdq` (long). Como último recurso, gerar hash determinístico.  
- **Data**: `data_hora_gmt` (datetime) — indexada para ordenação e filtros.  
- **Geo**: `geometry` (GeoJSON; CRS `EPSG:4326`).

Exemplo (simplificado):
```json
{
  "_id": "S-NPP_2025-10-03T14:20Z_-45.123_-12.345",
  "id": "S-NPP_2025-10-03T14:20Z_-45.123_-12.345",
  "data_hora_gmt": "2025-10-03T14:20:00Z",
  "properties": { ... todos os campos do WFS ... },
  "geometry": { "type":"Point", "coordinates":[-45.123,-12.345] }
}
```

## Rotina recomendada
- Agendamento a cada **10 min** para manter a janela de 48h atualizada.
- **Fallback**: se o WFS estiver indisponível momentaneamente, re-tenta com backoff exponencial.
- **Auditoria**: registrar `first_seen_at`, `last_seen_at` e `source_hash` para detectar mudanças.

---

## Índices no MongoDB

Criados em `app/deps.py`:
```python
await coll.create_index("id", unique=True)             # evita duplicatas (upsert)
await coll.create_index([("geometry", "2dsphere")])    # consultas espaciais
await coll.create_index([("data_hora_gmt", 1)])        # ordenação/consulta temporal
```

### Consultas Geo
- **Proximidade** (`$near`): “focos num raio de 25 km de (lon,lat)”.  
- **Polígono** (`$geoWithin` / `$geoIntersects`): “focos dentro de uma UC/município/bioma”.

> GeoJSON usa **[lon, lat]**. Mantemos a geometria como vem do WFS.

## Índices recomendados no Mongo
- Único em `id` (espelhado em `_id`): evita duplicidades quando reprocessamos janelas.
- Temporal em `view_date` (se presente) para ordenação/consultas.
- `2dsphere` em `geometry` para consultas espaciais (raio/polígono).

---

## Retenção / Sanitização (opcional)

- **TTL** (staging/cache): índice TTL em um campo `Date` (ex.: `expires_at`) para expirar docs automaticamente.  
- **Particionamento lógico**: coleções por período/uso — `focos_48h` (quente), `focos_YYYY` (histórico), `focos_archive` (frio).  
- **Janela ativa + pin**: mantenha N dias recentes e **pinned** (biomas/municípios críticos) sem expirar; remova docs pouco acessados (`last_accessed_at`) e não-pinned.

---

## Testes rápidos

### Pela API (sem Mongo)
- Schema de atributos:  
  `GET http://127.0.0.1:8000/debug/wfs-schema`
- Amostra (10 features):  
  `GET http://127.0.0.1:8000/debug/wfs-sample?limit=10`

### PowerShell (atenção ao `&`)
```powershell
# JSON direto do WFS (2.0.0)
curl.exe "https://terrabrasilis.dpi.inpe.br/queimadas/geoserver/wfs?service=WFS&version=2.0.0&request=GetFeature&typeNames=dados_abertos:focos_48h_br_satref&outputFormat=application/json&count=10&startIndex=0&sortBy=data_hora_gmt"
```

### Ingestão (com Mongo Atlas configurado)
```bash
curl -X POST http://127.0.0.1:8000/ingest/48h
```

---

## Troubleshooting

- **400 `Illegal property name` no `sortBy`**  
  Use `/debug/wfs-schema` para confirmar o campo de ordenação. O cliente tenta **sem `sortBy`** se o WFS rejeitar.

- **`bad auth : authentication failed` (Atlas)**  
  Verifique `MONGODB_URI` (user/senha/hostname). Em PowerShell, escape caracteres especiais e use aspas. O uso de `ServerApi('1')` ajuda a expor o erro exatamente na linha de conexão/ping.

- **Sem inserções, mas `write-test` funciona**  
  Em geral é mapeamento de chave (ex.: `foco_id`). No 48h, `_id = id = foco_id|id_foco_bdq`. O **mock** ajuda a checar contagens sem escrever.

- **PowerShell quebra com `&` na URL**  
  Use `curl.exe "url inteira com &"` (entre aspas).

---

## 9) Estrutura de pastas

```
│  └─ utils/
│     └─ time_windows.py
├─ pyproject.toml
├─ .env.example
└─ README.md

app/
  main.py                # FastAPI + middleware de request_id
  config.py              # Settings (Pydantic) + carregamento .env em camadas
  logging_config.py      # structlog + contextvars (JSON)
  scheduler.py           # APScheduler (cron) — agora usando função direta
  deps.py                # Conexão Mongo (ServerApi('1')) + criação de índices
  routers/
    ingest.py            # /ingest/48h (dry_run/mock_write), /ingest/initial, /ingest/incremental
    debug.py             # /debug/wfs-sample, /debug/wfs-schema
    data.py              # /data/recent, /data/find, /data/stats
    debug_data.py        # /data/debug/write-test e utilidades
  services/
    inpe_client.py       # Cliente WFS (httpx + tenacity + pybreaker; fallbacks)
  repositories/
    fires_repo.py        # Mapeamento e bulk upsert (UpdateOne)
  models/
    __init__.py          # Pydantic models (saídas/entradas) para as rotas
│ utils/
│   time_windows.py

```

---

## 10) `pyproject.toml` (Poetry)

Exemplo mínimo:
```toml
[tool.poetry]
name = "inpe-sync"
version = "0.1.0"
description = "INPE → Mongo sync API (FastAPI + Motor + APScheduler)"
authors = ["Seu Nome <voce@example.com>"]
readme = "README.md"
packages = [{ include = "app" }]

[tool.poetry.dependencies]
python = "^3.11"
fastapi = "^0.115.4"
uvicorn = {extras = ["standard"], version = "^0.30.6"}
httpx = "^0.27.2"
pydantic = "^2.9.2"
python-dotenv = "^1.0.1"
motor = "^3.6.0"
APScheduler = "^3.10.4"
structlog = "^24.1.0"
tenacity = "^9.0.0"
pybreaker = "^1.0.2"

[tool.poetry.group.dev.dependencies]
ipython = "^8.27.0"
pytest = "^8.3.3"

[tool.poetry.scripts]
inpe-sync = "app.main:app"
```

---

## 11) Operação

### Ingestão inicial
```bash
curl -X POST http://localhost:8000/ingest/initial
```

### Incremental on-demand
```bash
curl -X POST "http://localhost:8000/ingest/incremental?days=7"
```

### Agendado
Configure `SCHEDULE_CRON` (ex.: `*/15 * * * *` para a cada 15 min).

---

## 12) Segurança e produção

- Use **credenciais de leitura** em MongoDB Atlas com IP allowlist e/ou VPC peering.
- Centralize os **segredos** (.env/Secret Manager) e não comite o `.env`.
- Para produção, prefira **agendador externo** (K8s CronJob, Cloud Scheduler) chamando o endpoint incremental.
- Adicione **healthchecks** e **readiness/liveness** probes quando conteinerizar.

---

## 13) Troubleshooting

- **Muitos dados / timeouts**: reduza `WFS_PAGE_SIZE`, aumente timeout do httpx, adicione retries exponenciais.
- **Sobreposições**: garantidas via **upsert por chave** (`_id = gid` ou chave composta). Reprocessar janelas é seguro.
- **Sem `gid`**: escolha outra chave (por ex. `properties.id` ou hash determinístico de atributos estáveis) e ajuste o mapeador.

- **400 “Illegal property name”**  
  → O `sortBy` não existe na camada. Use `/debug/wfs-schema` e ajuste `WFS_SORTBY`.  
  → Nosso cliente já tenta **sem `sortBy`** e outras combinações.

- **PowerShell: `&` não permitido**  
  → Sempre coloque a URL entre **aspas** ou use `curl.exe` (não o alias `curl`).

- **`ServerSelectionTimeoutError localhost:27017`**  
  → Falta `MONGODB_URI` do Atlas no `.env` (ou não foi carregado).

- **Geo consultas “estranhas”**  
  → Verifique ordem `[lon, lat]` e índice `2dsphere` criado.

---

## 14) Customizações rápidas

- **Outra camada**: troque `WFS_SERVICE_PATH`, `WFS_TYPENAME`, `WFS_DATE_FIELD` no `.env`.
- **Chave composta**: gere `_id` como hash (`sha1` de `sat:lon:lat:timestamp`).
- **Auditoria**: grave difs em `focos_audit` antes de upserts.

---

## 15) Licenças e crédito de dados
Dados e serviços são disponibilizados pelo **INPE/TerraBrasilis** conforme as políticas do portal de dados. Verifique a licença específica da camada utilizada antes de redistribuir.

---