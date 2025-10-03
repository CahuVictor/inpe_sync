
# INPE → Mongo Sync (FastAPI · Poetry · Motor · APScheduler)

Sincroniza dados públicos do INPE (via **WFS/GeoServer** do TerraBrasilis) para o **MongoDB Atlas** com ingestão inicial e atualizações incrementais agendadas.

> API assíncrona (FastAPI + httpx), persistência (Motor) e agendamento interno (APScheduler). Pensada para **idempotência** (upsert por chave única), reprocessos de janelas sobrepostas e suporte a **consultas geoespaciais** (GeoJSON + índice `2dsphere`).

---

## 1) Por que Poetry?
O projeto é empacotado com **Poetry**, que gerencia dependências e ambiente virtual automaticamente.

### Requisitos
- Python 3.11+
- [Poetry](https://python-poetry.org/docs/#installation)

### Instalação
```bash
poetry install
```

### Executar em modo dev
```bash
poetry run uvicorn app.main:app --reload --port 8000
```

---

## 2) Configuração (.env)

Crie um arquivo `.env` na raiz a partir do `.env.example`:

```dotenv
# MongoDB Atlas
MONGODB_URI=mongodb+srv://<user>:<pass>@<cluster>/?retryWrites=true&w=majority
MONGODB_DB=inpe_db
MONGODB_COLLECTION=focos

# GeoServer/WFS TerraBrasilis
WFS_BASE=https://terrabrasilis.dpi.inpe.br/geoserver
WFS_SERVICE_PATH=/deter-amz/wfs        # caminho do workspace/serviço
WFS_TYPENAME=deter_public              # nome exato da camada (feature type)
WFS_DATE_FIELD=date                    # atributo de data (para filtros incrementais)
WFS_SRID=EPSG:4674                     # CRS do resultado
WFS_PAGE_SIZE=1000                     # paginação
WFS_SORTBY=gid                         # campo único para paginação estável

# Janela inicial (ingestão “full” inicial ou fatia histórica)
INITIAL_START=2019-01-01
INITIAL_END=2020-01-01

# Agendamento (cron): a cada 30 minutos
SCHEDULE_CRON=*/30 * * * *
```

> **Dica**: ajuste `WFS_SERVICE_PATH`, `WFS_TYPENAME` e `WFS_DATE_FIELD` conforme **a camada que você escolher** (DETER, PRODES, BDQueimadas, etc.). Veja o guia “Descobrir a camada” abaixo.

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


---

## 4) Endpoints da API

- `GET /health` → status
- `POST /ingest/initial` → ingestão inicial do intervalo `INITIAL_START..INITIAL_END`
- `POST /ingest/incremental?days=7` → janela incremental (rebusca “último visto” até agora)

O **agendador** dispara `incremental` conforme `SCHEDULE_CRON`.

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
Use `structlog` para JSON logs e correlação de chamadas:
```python
import structlog
log = structlog.get_logger()

log.info("wfs.fetch", start=start, end=end, start_index=start_index)
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

## 9) Estrutura de pastas

```
inpe-sync/
├─ app/
│  ├─ main.py
│  ├─ config.py
│  ├─ deps.py
│  ├─ scheduler.py
│  ├─ routers/
│  │  ├─ ingest.py
│  │  └─ health.py
│  ├─ services/
│  │  └─ inpe_client.py
│  ├─ repositories/
│  │  └─ fires_repo.py
│  └─ utils/
│     └─ time_windows.py
├─ pyproject.toml
├─ .env.example
└─ README.md
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

---

## 14) Customizações rápidas

- **Outra camada**: troque `WFS_SERVICE_PATH`, `WFS_TYPENAME`, `WFS_DATE_FIELD` no `.env`.
- **Chave composta**: gere `_id` como hash (`sha1` de `sat:lon:lat:timestamp`).
- **Auditoria**: grave difs em `focos_audit` antes de upserts.

---

## 15) Licenças e crédito de dados
Dados e serviços são disponibilizados pelo **INPE/TerraBrasilis** conforme as políticas do portal de dados. Verifique a licença específica da camada utilizada antes de redistribuir.

---

**Feito com ♥ para sincronizar dados do INPE com confiabilidade e consultas geo.**
