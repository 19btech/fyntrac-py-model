"""
fyntrac-py-model — FastAPI application entry point.

A Python microservice for querying the EventHistory MongoDB collection
with ZITADEL JWT authentication and multi-tenant database switching.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.db.mongodb import mongo_manager
from app.pulsar.manager import pulsar_manager
from app.routers import event_history

settings = get_settings()

# ── Logging ──────────────────────────────────────────────────────────────
def _get_log_level(level_str: str) -> int:
    """Map Java-style and Python log level strings to Python logging constants."""
    level_map = {
        "FINEST": logging.DEBUG,
        "FINER": logging.DEBUG,
        "FINE": logging.DEBUG,
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "SEVERE": logging.ERROR,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
        "FATAL": logging.CRITICAL
    }
    return level_map.get(level_str.upper(), logging.INFO)

log_level = _get_log_level(settings.LOG_LEVEL)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/tmp/fyntrac-py-model.log")
    ]
)
logger = logging.getLogger(__name__)


# ── Lifespan (startup + shutdown) ────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle — connect/disconnect MongoDB."""
    logger.info("Starting fyntrac-py-model service on port %s", settings.SERVICE_PORT)
    mongo_manager.connect(settings)
    logger.info("MongoDB client initialised")
    
    pulsar_manager.start(settings)
    
    yield
    
    await pulsar_manager.close()
    mongo_manager.close()
    logger.info("fyntrac-py-model service stopped")


# ── FastAPI app ──────────────────────────────────────────────────────────
app = FastAPI(
    title="fyntrac-py-model",
    description="Event History query service with ZITADEL SSO and multi-tenancy",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS ─────────────────────────────────────────────────────────────────
allowed_origins = [
    origin.strip()
    for origin in settings.CORS_ALLOWED_ORIGINS.split(",")
    if origin.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["*"],
    max_age=3600,
)

# ── Routers ──────────────────────────────────────────────────────────────
app.include_router(event_history.router)


# ── Health check ─────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"])
async def health_check():
    """Basic health check endpoint."""
    return {"status": "healthy", "service": "fyntrac-py-model"}


# ── Run with uvicorn (direct execution) ─────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.SERVICE_HOST,
        port=settings.SERVICE_PORT,
        reload=True,
    )
