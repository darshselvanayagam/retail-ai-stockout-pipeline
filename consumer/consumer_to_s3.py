import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "retail_events")


AWS_REGION = os.getenv("AWS_REGION", "ca-central-1")
S3_BUCKET = os.getenv("S3_BUCKET")          # set this
S3_PREFIX = os.getenv("S3_PREFIX", "retail/raw")

LOCAL_DIR = Path("./_buffer")
LOCAL_DIR.mkdir(exist_ok=True)

BATCH_SIZE = 5000    # basically ignore size
FLUSH_SECONDS = 60*60     # 1 hour (60 × 60)
      

def utc_now():
    return datetime.now(timezone.utc)

def s3_key_for_date(date_str: str) -> str:
    y, m, d = date_str.split("-")
    fname = f"events_{date_str}_{int(time.time())}.jsonl"
    return f"{S3_PREFIX}/{y}/{m}/{d}/{fname}"

def upload_file(s3_client, local_path: Path, date_str: str):
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET env var is not set.")
    key = s3_key_for_date(date_str)
    s3_client.upload_file(str(local_path), S3_BUCKET, key)
    print(f" Uploaded to s3://{S3_BUCKET}/{key}")

def flush_to_s3(s3_client, buffer, date_str: str):
    if not buffer:
        return

    local_path = LOCAL_DIR / f"events_{date_str}.jsonl"
    with local_path.open("a", encoding="utf-8") as f:
        for e in buffer:
            f.write(json.dumps(e) + "\n")

    upload_file(s3_client, local_path, date_str)

    # clean up local file for next batch
    local_path.unlink(missing_ok=True)

if __name__ == "__main__":
    if not S3_BUCKET:
        print("❌ Please set S3_BUCKET before running.")
        print("Example: export S3_BUCKET='my-bucket-name'")
        raise SystemExit(1)

    s3 = boto3.client("s3", region_name=AWS_REGION)

    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "retail-consumer-to-s3",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe([TOPIC])

    print(f"Consuming '{TOPIC}' from {BOOTSTRAP} and writing to S3 bucket {S3_BUCKET}")

    buffer = []
    last_flush = time.time()
    current_date = utc_now().date().isoformat()

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                print("Kafka error:", msg.error())
            else:
                event = json.loads(msg.value().decode("utf-8"))
                print("Consumed:", event, flush=True)
                event_date = event.get("event_date") or current_date
                current_date = event_date
                buffer.append(event)

            if len(buffer) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_SECONDS:
                flush_to_s3(s3, buffer, current_date)
                buffer = []
                last_flush = time.time()

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        if buffer:
            flush_to_s3(s3, buffer, current_date)
        c.close()
        print("Consumer stopped.")
