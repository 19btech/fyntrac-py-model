"""
REST API endpoint for querying the EventHistory collection.

GET /event-history
  Query params: instrumentId, attributeId, postingDate
  Header:       X-Tenant-ID (required)
  Auth:         Bearer JWT (ZITADEL)
  Sort:         priority DESC
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import DESCENDING

from app.auth.jwt_bearer import verify_jwt
from app.db.mongodb import get_tenant_db

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Event History"])

COLLECTION_NAME = "EventHistory"


@router.get("/event-history", response_model=list[dict])
async def get_event_history(
    instrumentId: Optional[str] = Query(None, description="Filter by instrument ID"),
    attributeId: Optional[str] = Query(None, description="Filter by attribute ID"),
    postingDate: Optional[int] = Query(None, description="Filter by posting date"),
    db: AsyncIOMotorDatabase = Depends(get_tenant_db),
    _token_payload: dict = Depends(verify_jwt),
):
    """
    Retrieve EventHistory documents with optional filters, sorted by priority DESC.

    - **instrumentId**: exact match on the instrumentId field
    - **attributeId**: exact match on the attributeId field
    - **postingDate**: exact match on the postingDate field (integer)

    The target MongoDB database is determined by the `X-Tenant-ID` request header.
    """
    # Build filter dynamically from non-null parameters
    query_filter: dict = {}
    if instrumentId is not None:
        query_filter["instrumentId"] = instrumentId
    if attributeId is not None:
        query_filter["attributeId"] = attributeId
    if postingDate is not None:
        query_filter["postingDate"] = postingDate

    logger.info(
        "Querying %s with filter=%s on db=%s",
        COLLECTION_NAME,
        query_filter,
        db.name,
    )

    collection = db[COLLECTION_NAME]
    cursor = collection.find(query_filter).sort("priority", DESCENDING)

    results = []
    async for document in cursor:
        # Convert ObjectId to string for JSON serialisation
        if "_id" in document:
            document["_id"] = str(document["_id"])
        results.append(document)

    logger.info("Returning %d documents", len(results))
    return results
