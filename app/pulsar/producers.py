"""
Pulsar producers that mirror the Java-side:
  - AggregationMessageProducer  → topic: fyntrac-aggregate-execution
  - GeneralLedgerMessageProducer → topic: fyntrac-book-gl-staging

Message schemas match Records.ExecuteAggregationMessageRecord and
Records.GeneralLedgerMessageRecord in the subledger common module.
"""
import json
import logging
import asyncio
from typing import Optional

import pulsar

logger = logging.getLogger(__name__)


class AggregationMessageProducer:
    """Publishes ExecuteAggregationMessageRecord to fyntrac-aggregate-execution.

    Java equivalent record:
        record ExecuteAggregationMessageRecord(String tenantId, Long jobId, Long aggregationDate)
    """

    def __init__(self, client: pulsar.Client, topic: str):
        self._producer: Optional[pulsar.Producer] = None
        self._client = client
        self._topic = topic

    def _ensure_producer(self) -> pulsar.Producer:
        if self._producer is None:
            self._producer = self._client.create_producer(self._topic)
        return self._producer

    async def execute_aggregation(
        self, tenant_id: str, job_id: int, aggregation_date: int, loop: asyncio.AbstractEventLoop
    ) -> None:
        """Publish an aggregation trigger message.

        Args:
            tenant_id:        Tenant identifier string.
            job_id:           Long job identifier (epoch ms from Java).
            aggregation_date: Posting date as an int (YYYYMMDD).
            loop:             Running asyncio event loop.
        """
        payload = {
            "tenantId": tenant_id,
            "jobId": job_id,
            "aggregationDate": aggregation_date,
        }
        try:
            producer = self._ensure_producer()
            data = json.dumps(payload).encode("utf-8")
            await loop.run_in_executor(None, producer.send, data)
            logger.info(
                "AggregationMessageProducer: published executeAggregation "
                "tenantId=%s jobId=%s aggregationDate=%s",
                tenant_id, job_id, aggregation_date,
            )
        except Exception as exc:
            logger.error(
                "AggregationMessageProducer: failed to publish message: %s", exc
            )
            raise

    def close(self) -> None:
        if self._producer:
            try:
                self._producer.close()
            except Exception:
                pass
            self._producer = None


class GeneralLedgerMessageProducer:
    """Publishes GeneralLedgerMessageRecord to fyntrac-book-gl-staging.

    Java equivalent record:
        record GeneralLedgerMessageRecord(String tenantId, Long jobId)
    """

    def __init__(self, client: pulsar.Client, topic: str):
        self._producer: Optional[pulsar.Producer] = None
        self._client = client
        self._topic = topic

    def _ensure_producer(self) -> pulsar.Producer:
        if self._producer is None:
            self._producer = self._client.create_producer(self._topic)
        return self._producer

    async def book_temp_gl(
        self, tenant_id: str, job_id: int, loop: asyncio.AbstractEventLoop
    ) -> None:
        """Publish a GL booking trigger message.

        Args:
            tenant_id: Tenant identifier string.
            job_id:    Long job identifier (epoch ms from Java).
            loop:      Running asyncio event loop.
        """
        payload = {
            "tenantId": tenant_id,
            "jobId": job_id,
        }
        try:
            producer = self._ensure_producer()
            data = json.dumps(payload).encode("utf-8")
            await loop.run_in_executor(None, producer.send, data)
            logger.info(
                "GeneralLedgerMessageProducer: published bookTempGL "
                "tenantId=%s jobId=%s",
                tenant_id, job_id,
            )
        except Exception as exc:
            logger.error(
                "GeneralLedgerMessageProducer: failed to publish message: %s", exc
            )
            raise

    def close(self) -> None:
        if self._producer:
            try:
                self._producer.close()
            except Exception:
                pass
            self._producer = None
