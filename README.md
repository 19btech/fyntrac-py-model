# fyntrac-py-model

A Python FastAPI microservice for querying the **EventHistory** MongoDB collection with ZITADEL SSO authentication and multi-tenant database switching.

## Features

- **ZITADEL JWT Authentication** — validates Bearer tokens using JWKS public keys
- **Multi-Tenant MongoDB** — dynamically switches database based on `X-Tenant-ID` header
- **EventHistory API** — query with filters (`instrumentId`, `attributeId`, `postingDate`) and sort by `priority DESC`
- **Docker support** — production-ready Dockerfile and docker-compose.yml

## Quick Start

### Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your ZITADEL and MongoDB settings

# Run the service
uvicorn app.main:app --host 0.0.0.0 --port 8090 --reload
```

### Docker

```bash
# Build and run
docker compose up --build

# Or build only
docker build -t fyntrac-py-model .
```

## API

### Health Check

```
GET /health
```

### Event History

```
GET /event-history?instrumentId=XXX&attributeId=YYY&postingDate=20240101
```

**Headers:**
| Header | Required | Description |
|--------|----------|-------------|
| `Authorization` | Yes | `Bearer <JWT>` |
| `X-Tenant-ID` | Yes | MongoDB database name |

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `instrumentId` | string | No | Filter by instrument ID |
| `attributeId` | string | No | Filter by attribute ID |
| `postingDate` | integer | No | Filter by posting date |

**Response:** JSON array of EventHistory documents, sorted by `priority DESC`.

## Configuration

All settings are configured via environment variables. See [.env.example](.env.example) for the full list.

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE_PORT` | `8090` | Service port |
| `MONGODB_HOST` | `127.0.0.1` | MongoDB host |
| `MONGODB_PORT` | `27017` | MongoDB port |
| `MONGODB_USERNAME` | `root` | MongoDB username |
| `MONGODB_PASSWORD` | — | MongoDB password |
| `MONGODB_AUTH_DATABASE` | `admin` | MongoDB auth database |
| `MONGODB_DEFAULT_DATABASE` | `master` | Default database name |
| `ZITADEL_ISSUER_URI` | — | ZITADEL issuer URI |
| `ZITADEL_PROJECT_ID` | — | ZITADEL project ID for audience validation |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:3030` | Comma-separated CORS origins |
| `LOG_LEVEL` | `INFO` | Logging level |
