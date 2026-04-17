import logging
import json
import asyncio
from typing import Optional

import pulsar
from pulsar import Timeout
from pymemcache.client.base import Client as MemcacheClient

from app.config import Settings
from app.db.mongodb import mongo_manager

logger = logging.getLogger(__name__)


class PulsarManager:
    """Manages the Pulsar client and background consumer task."""

    def __init__(self):
        self._client: Optional[pulsar.Client] = None
        self._settings: Optional[Settings] = None
        self._consumer_task: Optional[asyncio.Task] = None
        self._python_model_consumer_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._memcache_client: Optional[MemcacheClient] = None

    def start(self, settings: Settings) -> None:
        """Initialise Pulsar client and start consumer loop."""
        if not settings.PULSAR_SERVICE_URL:
            logger.warning("PULSAR_SERVICE_URL not set; skipping Pulsar integration.")
            return

        self._settings = settings
        logger.info("Connecting to Pulsar at %s", settings.PULSAR_SERVICE_URL)
        
        try:
            self._client = pulsar.Client(settings.PULSAR_SERVICE_URL)
            self._stop_event.clear()
            
            # Setup memcached client
            self._memcache_client = MemcacheClient(
                (settings.MEMCACHED_HOST, settings.MEMCACHED_PORT),
                connect_timeout=5,
                timeout=5
            )

            # Start the background tasks within the current asyncio event loop
            loop = asyncio.get_running_loop()
            self._consumer_task = loop.create_task(self._consumer_loop())
            self._python_model_consumer_task = loop.create_task(self._python_model_consumer_loop())
            logger.info("Pulsar background consumers (EventHistory + PythonModel) and Memcached connected.")
        except Exception as e:
            logger.error("Failed to start Pulsar/Memcache manager: %s", e)

    async def close(self) -> None:
        """Signal background tasks to stop and close client."""
        logger.info("Stopping Pulsar manager...")
        self._stop_event.set()
        
        for task_name, task in [("EventHistory", self._consumer_task),
                                ("PythonModel", self._python_model_consumer_task)]:
            if task:
                try:
                    await asyncio.wait_for(task, timeout=5.0)
                except asyncio.TimeoutError:
                    logger.warning("%s consumer task took too long. Cancelling...", task_name)
                    task.cancel()
                except Exception as e:
                    logger.error("Error during %s consumer shutdown: %s", task_name, e)
                
        if self._client:
            self._client.close()
            logger.info("Pulsar connection closed.")
            
        if self._memcache_client:
            self._memcache_client.close()
            logger.info("Memcache connection closed.")

    async def _consumer_loop(self):
        """Background loop that polls for messages and processes them."""
        logger.info("Subscribing to topic: %s", self._settings.PULSAR_EVENT_HISTORY_QUERY_TOPIC)
        
        try:
            consumer = self._client.subscribe(
                self._settings.PULSAR_EVENT_HISTORY_QUERY_TOPIC,
                subscription_name=self._settings.PULSAR_SUBSCRIPTION_NAME,
                consumer_type=pulsar.ConsumerType.Shared,
            )
            
            producer = self._client.create_producer(self._settings.PULSAR_EVENT_HISTORY_RESULT_TOPIC)
        except Exception as e:
             logger.error("Failed to create Pulsar consumer/producer: %s", e)
             return

        loop = asyncio.get_running_loop()

        while not self._stop_event.is_set():
            try:
                # receive(timeout_millis) runs in executor to avoid blocking the async event loop
                msg = await loop.run_in_executor(None, consumer.receive, 1000)
            except Timeout:
                # Normal timeout when no messages are available, just loop again
                continue
            except Exception as e:
                logger.error("Error receiving from Pulsar: %s", e)
                await asyncio.sleep(1)
                continue

            try:
                payload = json.loads(msg.data().decode('utf-8'))
                logger.info("Received Pulsar message: tenantId=%s", payload.get("tenantId"))
                
                # Process message async and await the result
                await self._process_query(payload, producer)
                
                # Acknowledge successfully processed message
                consumer.acknowledge(msg)
            except json.JSONDecodeError as e:
                logger.error("Failed to decode message JSON: %s", e)
                consumer.acknowledge(msg) # Ack invalid messages so they are discarded
            except Exception as e:
                logger.error("Error processing Pulsar message: %s", e, exc_info=True)
                # Negative acknowledge to retry
                consumer.negative_acknowledge(msg)
                
        consumer.close()
        producer.close()

    async def _process_query(self, payload: dict, producer: pulsar.Producer):
        """Handle an incoming batched query and publish back results."""
        tenant_id = payload.get("tenantId")
        instrument_ids = payload.get("instrumentIds", [])
        job_id = payload.get("jobId")
        posting_date = payload.get("postingDate")

        if not tenant_id or not instrument_ids:
            logger.warning("Invalid payload: missing tenantId or instrumentIds. Payload: %s", payload)
            return

        # Fetch tenant database
        try:
            db = mongo_manager.get_database(tenant_id)
        except Exception as e:
            logger.error("Failed to get DB for tenant %s: %s", tenant_id, e)
            return

        collection = db["EventHistory"]
        tracker_collection = db["EventHistoryBatchTracker"]
        
        # Mark as PROCESSING
        try:
            from datetime import datetime, timezone
            await tracker_collection.update_one(
                {"jobId": job_id, "tenantId": tenant_id},
                {"$set": {"status": "PROCESSING", "updatedAt": datetime.now(timezone.utc)}}
            )
        except Exception as e:
            logger.warning("Failed to update tracking to PROCESSING for job %s: %s", job_id, e)

        # Build dynamic query
        # Use exact match for $in since there are batched IDs
        query = {"instrumentId": {"$in": instrument_ids}}
        if posting_date is not None:
            query["postingDate"] = posting_date

        logger.info("Querying EventHistory for %d instruments in tenant %s", len(instrument_ids), tenant_id)
        
        # We sort by descending priority just like the REST endpoint
        results = []
        try:
            async for doc in collection.find(query).sort("priority", -1):
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])  # serialize ObjectId
                results.append(doc)
        except Exception as e:
            logger.error("Database query failed: %s", e)
            try:
                from datetime import datetime, timezone
                await tracker_collection.update_one(
                    {"jobId": job_id, "tenantId": tenant_id},
                    {"$set": {
                        "status": "FAILED", 
                        "errorMessage": str(e),
                        "updatedAt": datetime.now(timezone.utc)
                    }}
                )
            except Exception as update_err:
                logger.error("Failed to mark FAILED status: %s", update_err)
            raise e # Reraise to negatively acknowledge message

        logger.info("Found %d events for batch. Serializing to Memcached.", len(results))

        cache_key = f"event-history-{tenant_id}-{job_id}"
        
        # We push to memcached synchronously or via run_in_executor
        try:
            results_json = json.dumps(results)
            loop = asyncio.get_running_loop()
            # Push to memcached with an expiration of 1 hour (3600 seconds)
            await loop.run_in_executor(None, self._memcache_client.set, cache_key, results_json.encode('utf-8'), 3600)
            logger.info("Successfully pushed results to Memcached at key: %s", cache_key)
        except Exception as e:
            logger.error("Failed to push results to Memcached: %s", e)
            try:
                from datetime import datetime, timezone
                await tracker_collection.update_one(
                    {"jobId": job_id, "tenantId": tenant_id},
                    {"$set": {
                        "status": "FAILED", 
                        "errorMessage": f"Memcached Error: {str(e)}",
                        "updatedAt": datetime.now(timezone.utc)
                    }}
                )
            except Exception as update_err:
                pass
            raise e

        # Mark as COMPLETED in Mongo Tracker
        try:
            from datetime import datetime, timezone
            await tracker_collection.update_one(
                {"jobId": job_id, "tenantId": tenant_id},
                {"$set": {
                    "status": "COMPLETED", 
                    "processedInstrumentCount": len(results),
                    "updatedAt": datetime.now(timezone.utc)
                }}
            )
        except Exception as update_err:
            logger.warning("Failed to mark tracker as COMPLETED for job %s: %s", job_id, update_err)

        # Build result payload
        result_payload = {
            "tenantId": tenant_id,
            "jobId": job_id,
            "success": True,
            "error": None,
            "cacheKey": cache_key,
            "resultCount": len(results)
        }

        # Block to send the result (or run in executor to avoid blocking loop if network is slow)
        try:
            encoded_data = json.dumps(result_payload).encode('utf-8')
            await loop.run_in_executor(None, producer.send, encoded_data)
        except Exception as e:
             logger.error("Failed to publish result to Pulsar: %s", e)
             raise e

    async def _python_model_consumer_loop(self):
        """Background loop that polls for Python model execution messages."""
        topic = self._settings.PULSAR_PYTHON_MODEL_EXECUTION_TOPIC
        subscription = self._settings.PULSAR_PYTHON_MODEL_SUBSCRIPTION_NAME
        logger.info("Subscribing to Python model execution topic: %s", topic)

        try:
            consumer = self._client.subscribe(
                topic,
                subscription_name=subscription,
                consumer_type=pulsar.ConsumerType.Shared,
            )
        except Exception as e:
            logger.error("Failed to create Python model consumer: %s", e)
            return

        loop = asyncio.get_running_loop()

        while not self._stop_event.is_set():
            try:
                msg = await loop.run_in_executor(None, consumer.receive, 1000)
            except Timeout:
                continue
            except Exception as e:
                logger.error("Error receiving Python model message: %s", e)
                await asyncio.sleep(1)
                continue

            try:
                payload = json.loads(msg.data().decode('utf-8'))
                logger.info("Received Python model execution message: tenantId=%s, executionDate=%s, key=%s",
                            payload.get("tenantId"), payload.get("executionDate"), payload.get("key"))

                await self._process_python_model_execution(payload)

                consumer.acknowledge(msg)
            except json.JSONDecodeError as e:
                logger.error("Failed to decode Python model message JSON: %s", e)
                consumer.acknowledge(msg)
            except Exception as e:
                logger.error("Error processing Python model message: %s", e, exc_info=True)
                consumer.negative_acknowledge(msg)

        consumer.close()

    async def _process_python_model_execution(self, payload: dict):
        """Handle an incoming Python model execution message.
        
        Reads instrument IDs from Memcached (via the cache key in the message),
        then executes the Python model logic for those instruments.
        """
        tenant_id = payload.get("tenantId")
        execution_date = payload.get("executionDate")
        cache_key = payload.get("key")
        is_last = payload.get("isLast", False)

        if not tenant_id or not cache_key:
            logger.warning("Invalid Python model payload: missing tenantId or key. Payload: %s", payload)
            return

        # Read instrument IDs from Memcached
        instrument_ids = []
        try:
            loop = asyncio.get_running_loop()
            cached_data = await loop.run_in_executor(None, self._memcache_client.get, cache_key)
            if cached_data:
                instrument_ids = json.loads(cached_data.decode('utf-8'))
                logger.info("Retrieved %d instrument IDs from Memcached key: %s", len(instrument_ids), cache_key)
            else:
                logger.warning("No data found in Memcached for key: %s", cache_key)
                return
        except Exception as e:
            logger.error("Failed to read from Memcached key %s: %s", cache_key, e)
            return

        # Fetch tenant database
        try:
            db = mongo_manager.get_database(tenant_id)
        except Exception as e:
            logger.error("Failed to get DB for tenant %s: %s", tenant_id, e)
            return

        # Execute Python model for each instrument
        await self._execute_python_model(db, tenant_id, execution_date, instrument_ids)

        if is_last:
            logger.info("Last batch processed for Python model execution. Tenant=%s, Date=%s",
                        tenant_id, execution_date)

    async def _execute_python_model(self, db, tenant_id: str, execution_date: int, instrument_ids: list):
        """Execute the Python model logic for a batch of instruments in parallel.

        Each instrument is processed in a separate thread using a ThreadPoolExecutor 
        to achieve true parallelism for CPU-bound model execution tasks.
        """
        collection = db["EventHistory"]
        max_concurrency = min(32, (os.cpu_count() or 1) * 4)  # Reasonable thread pool size
        
        logger.info("Executing Python model for %d instruments in parallel (threads=%d) "
                     "tenant=%s postingDate=%s",
                     len(instrument_ids), max_concurrency, tenant_id, execution_date)

        # We need a synchronous function to run in the thread pool that handles the async event loop
        # But wait, collection.find() is async (Motor). So we need to query data asynchronously FIRST,
        # OR we isolate the CPU-bound portion to be run in executor!
        
        # Let's query events async first, then pass the data to threads for CPU-bound execution.
        async def fetch_instrument_data(instrument_id: str):
            try:
                query = {"instrumentId": instrument_id}
                if execution_date is not None:
                    query["postingDate"] = execution_date

                events = []
                async for doc in collection.find(query).sort("priority", -1):
                    if "_id" in doc:
                        doc["_id"] = str(doc["_id"])
                    events.append(doc)
                return instrument_id, events
            except Exception as e:
                logger.error("Error fetching data for %s: %s", instrument_id, e)
                return instrument_id, None

        logger.info("Fetching data for %d instruments from MongoDB...", len(instrument_ids))
        
        # Fetch all instrument data concurrently (I/O bound)
        fetch_tasks = [fetch_instrument_data(iid) for iid in instrument_ids]
        fetched_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
        
        loop = asyncio.get_running_loop()
        success_count = 0
        
        import concurrent.futures
        
        # Now run the CPU-bound processing in a ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrency) as pool:
            process_futures = []
            
            for result in fetched_results:
                if isinstance(result, Exception) or result is None:
                    continue
                
                instrument_id, events = result
                if not events:
                    continue
                    
                # Submit synchronous CPU-bound task to thread pool
                future = loop.run_in_executor(
                    pool,
                    self._process_instrument_sync,
                    tenant_id,
                    instrument_id,
                    execution_date,
                    events
                )
                process_futures.append(future)
            
            if process_futures:
                # Wait for all threads to complete
                results = await asyncio.gather(*process_futures, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        logger.error("Thread pool execution error: %s", r)
                    elif r:
                        success_count += 1
                        
        logger.info(
            "Python model: completed batch. Instruments=%d, Succeeded=%d, Failed=%d, "
            "Tenant=%s, PostingDate=%s",
            len(instrument_ids), success_count, len(instrument_ids) - success_count,
            tenant_id, execution_date
        )

    def _process_instrument_sync(self, tenant_id: str, instrument_id: str, posting_date: int, events: list) -> bool:
        """Synchronous method executed in a separate thread for CPU-bound processing."""
        try:
            logger.info("Instrument %s: processing %d events (thread=%s)", 
                        instrument_id, len(events), threading.current_thread().name)
            
            # Group events by attributeId
            events_by_attribute = {}
            for event in events:
                attribute_id = self._extract_attribute_id(event)
                if attribute_id not in events_by_attribute:
                    events_by_attribute[attribute_id] = []
                events_by_attribute[attribute_id].append(event)

            # Process each group
            for attribute_id, attribute_events in events_by_attribute.items():
                self._process_instrument_attribute_sync(
                    tenant_id, instrument_id, attribute_id, posting_date, attribute_events
                )
            
            return True
        except Exception as e:
            logger.error("Error in thread processing instrument %s: %s", instrument_id, e, exc_info=True)
            return False

    def _extract_attribute_id(self, event: dict) -> str:
        """Extract attributeId from an EventHistory document.

        The attributeId may be:
        1. A top-level field on the document
        2. Nested inside eventDetail.values as a key or value
        Falls back to 'default' if not found.
        """
        # Check top-level field first
        attr_id = event.get("attributeId")
        if attr_id:
            return str(attr_id)

        # Check inside eventDetail.values
        event_detail = event.get("eventDetail")
        if event_detail and isinstance(event_detail, dict):
            values = event_detail.get("values")
            if values and isinstance(values, dict):
                # values is Map<String, Map<String, Object>>
                # Look for attributeId in the inner maps
                for source_key, value_map in values.items():
                    if isinstance(value_map, dict):
                        aid = value_map.get("attributeId")
                        if aid:
                            return str(aid)

        return "default"

    def _process_instrument_attribute_sync(self, tenant_id: str, instrument_id: str,
                                             attribute_id: str, posting_date: int,
                                             events: list):
        """Synchronous version of attribute processing (executed in thread)."""
        logger.info(
            "Thread processing: tenant=%s, instrument=%s, attribute=%s, postingDate=%s, events=%d",
            tenant_id, instrument_id, attribute_id, posting_date, len(events)
        )

        for event in events:
            event_name = event.get("eventName", "unknown")
            event_id = event.get("eventId", "")
            effective_date = event.get("effectiveDate")
            priority = event.get("priority", 0)

            event_values = {}
            event_detail = event.get("eventDetail")
            if event_detail and isinstance(event_detail, dict):
                values = event_detail.get("values")
                if values and isinstance(values, dict):
                    for source_key, field_map in values.items():
                        if isinstance(field_map, dict):
                            event_values.update(field_map)

            logger.debug(
                "  Event: name=%s, id=%s, effectiveDate=%s, priority=%d",
                event_name, event_id, effective_date, priority
            )

            # TODO: CPU-BOUND Python model logic goes here
            pass



# Create singleton instance
pulsar_manager = PulsarManager()
