import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

import os
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "retail_events")


producer = Producer({"bootstrap.servers": BOOTSTRAP})

PRODUCTS = ["milk", "bread", "eggs", "soap", "shampoo"]
STORES = ["store_1", "store_2", "store_3"]

def now_utc():
    return datetime.now(timezone.utc)

def make_event():
    t = now_utc()
    event_date = t.date().isoformat()

    if random.random() < 0.7:
        return {
            "event_type": "sale",
            "event_time": t.isoformat(),
            "event_date": event_date,
            "store_id": random.choice(STORES),
            "product_id": random.choice(PRODUCTS),
            "qty": random.randint(1, 4)
        }
    else:
        return {
            "event_type": "inventory",
            "event_time": t.isoformat(),
            "event_date": event_date,
            "store_id": random.choice(STORES),
            "product_id": random.choice(PRODUCTS),
            "on_hand": random.randint(0, 20)
        }

print("Producing events to Kafka...")

while True:
    event = make_event()
    producer.produce(TOPIC, json.dumps(event).encode("utf-8"))
    producer.poll(0)
    print("Sent:", event)
    time.sleep(60)
