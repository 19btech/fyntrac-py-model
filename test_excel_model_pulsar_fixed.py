import pulsar
import json
import uuid

PULSAR_SERVICE_URL = 'pulsar://localhost:6650'
PULSAR_TOPIC = 'persistent://public/default/fyntrac-python-model-execution'
TNT001 = "TNT001"

def test_producer():
    print("🚀 Starting Pulsar Producer Test script (Fan-Out Direct Payload)...")
    job_id = f"job-{uuid.uuid4()}"
    print(f"Connecting to Pulsar at {PULSAR_SERVICE_URL}...")
    try:
        client = pulsar.Client(PULSAR_SERVICE_URL)
        producer = client.create_producer(PULSAR_TOPIC)
        chunks = [
            ["LOAN2"],
            ["LOAN3", "LOAN1"]
        ]
        for i, chunk in enumerate(chunks):
            is_last = (i == len(chunks) - 1)
            payload = {
                "tenantId": TNT001,
                "executionDate": 20220131,
                "jobId": job_id,
                "chunkIndex": i,
                "instrumentIds": chunk,
                "isLast": is_last
            }
            print(f"📤 Publishing chunk {i} to {PULSAR_TOPIC} \n   Instruments: {len(chunk)}")
            producer.send(json.dumps(payload).encode('utf-8'))
        print("✅ All chunk messages published successfully!")
        producer.close()
        client.close()
    except Exception as e:
        print(f"❌ Failed to connect or write to Pulsar: {e}")

if __name__ == "__main__":
    test_producer()
