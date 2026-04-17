"""
Pydantic models for the EventHistory MongoDB collection.

Mirrors the Java entity definitions:
  - Event.java        → EventHistoryResponse
  - EventDetail.java  → EventDetailModel
  - EventStatus.java  → EventStatus enum
"""

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class EventStatus(str, Enum):
    """Maps to com.fyntrac.common.enums.EventStatus."""
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    ERROR_OUT = "ERROR_OUT"


class EventDetailModel(BaseModel):
    """Maps to com.fyntrac.common.entity.EventDetail."""
    sourceTable: Optional[str] = None
    sourceType: Optional[str] = None
    sourceKey: Optional[str] = None
    isAscendingOrder: Optional[bool] = None
    values: Optional[Dict[str, Dict[str, Any]]] = None


class EventHistoryResponse(BaseModel):
    """
    Response model for documents in the EventHistory collection.
    Maps to com.fyntrac.common.entity.Event.
    """
    id: Optional[str] = Field(None, alias="_id")
    instrumentId: str
    eventId: str
    eventName: Optional[str] = None
    postingDate: int
    effectiveDate: int
    lastPlayedPostingDate: int
    priority: int
    status: Optional[str] = None
    eventDetail: Optional[EventDetailModel] = None
    # attributeId may exist in the MongoDB documents as a runtime field
    attributeId: Optional[str] = None

    class Config:
        populate_by_name = True
        json_encoders = {
            # ObjectId serialisation handled by the router
        }
