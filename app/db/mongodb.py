"""
Multi-tenant MongoDB connection manager.

Replicates the pattern from MultiTenantMongoDbFactory.java and
TenantDatasourceConfig.java in the subledger/common module.

Uses Motor (async PyMongo) for FastAPI compatibility.
A single MongoClient is shared; tenant isolation is achieved by
switching the database name based on the X-Tenant-ID request header.
"""

import logging
from typing import Optional

from fastapi import Depends, Header, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import Settings, get_settings

logger = logging.getLogger(__name__)


class MongoManager:
    """
    Manages a single Motor client and provides tenant-scoped database access.
    """

    def __init__(self):
        self._client: Optional[AsyncIOMotorClient] = None
        self._settings: Optional[Settings] = None

    def connect(self, settings: Settings) -> None:
        """Initialise the Motor client from application settings."""
        self._settings = settings
        uri = settings.mongodb_connection_uri
        logger.info(
            "Connecting to MongoDB at %s:%s",
            settings.MONGODB_HOST,
            settings.MONGODB_PORT,
        )
        self._client = AsyncIOMotorClient(
            uri,
            maxPoolSize=20,
            minPoolSize=5,
            serverSelectionTimeoutMS=5000,
        )

    def close(self) -> None:
        """Close the Motor client."""
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")

    def get_database(self, tenant_id: str) -> AsyncIOMotorDatabase:
        """
        Return the Motor database for the given tenant.

        The tenant ID is used directly as the database name, mirroring
        the Java MultiTenantMongoDbFactory behaviour.
        """
        if self._client is None:
            raise RuntimeError("MongoManager is not connected")
        return self._client[tenant_id]

    def get_default_database(self) -> AsyncIOMotorDatabase:
        """Return the default (master) database."""
        if self._client is None:
            raise RuntimeError("MongoManager is not connected")
        return self._client[self._settings.MONGODB_DEFAULT_DATABASE]


# ── Singleton instance ───────────────────────────────────────────────────
mongo_manager = MongoManager()


async def get_tenant_db(
    x_tenant_id: Optional[str] = Header(None, alias="X-Tenant-ID"),
    settings: Settings = Depends(get_settings),
) -> AsyncIOMotorDatabase:
    """
    FastAPI dependency that extracts X-Tenant-ID from the request header
    and returns the corresponding Motor database.

    Returns 400 if the header is missing.
    """
    if not x_tenant_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Tenant-ID header is required",
        )

    logger.debug("Resolving database for tenant: %s", x_tenant_id)
    return mongo_manager.get_database(x_tenant_id)
