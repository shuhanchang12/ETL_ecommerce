"""simple_producer.py
Simple Kafka producer using confluent-kafka
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# Configuration
BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "order_status_event")

# Create producer
producer = Producer({
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'client.id': 'python-producer'
})

statuses = [
    ("CREATED", 0.3),
    ("PAID", 0.25),
    ("PACKED", 0.15),
    ("SHIPPED", 0.1),
    ("DELIVERED", 0.05),
    ("CANCELLED", 0.05),
    ("REFUNDED", 0.05),
    ("RETURNED", 0.05),
]
rng = random.Random(42)


def create_order_status_event(order_id, customer_id, status, timestamp=None):
    """Create an order status event."""
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": order_id,
        "customer_id": customer_id,
        "new_status": status,
        "status_ts": timestamp,
        "source": "order_service",
    }


def delivery_report(err, msg):
    """Callback for message delivery."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def stream_order_events(df, delay=0.5):
    """Stream order status events for each order."""
    print(f"Streaming events for {len(df)} orders to topic '{TOPIC_NAME}'...")
    
    status_sequence = [s for s, _ in statuses if s not in ("CANCELLED", "REFUNDED", "RETURNED")]
    
    for _, row in df.iterrows():
        order_id = row.get("order_id")
        customer_id = row.get("customer_id")
        
        # Create realistic status progression
        max_idx = random.randint(2, len(status_sequence))
        
        for status in status_sequence[:max_idx]:
            evt = create_order_status_event(order_id, customer_id, status)
            producer.produce(
                TOPIC_NAME,
                key=str(order_id),
                value=json.dumps(evt).encode('utf-8'),
                callback=delivery_report
            )
            print(f"→ Order {order_id}: {status}")
            time.sleep(delay)

        # Sometimes add terminal event
        if random.random() < 0.15:
            terminal_status = random.choice([s for s in ("CANCELLED", "REFUNDED", "RETURNED")])
            evt = create_order_status_event(order_id, customer_id, terminal_status)
            producer.produce(
                TOPIC_NAME,
                key=str(order_id),
                value=json.dumps(evt).encode('utf-8'),
                callback=delivery_report
            )
            print(f"→ Order {order_id}: {terminal_status}")
            time.sleep(delay)
    
    producer.flush()
    print("✅ All events sent successfully!")


if __name__ == "__main__":
    # Load orders data
    path_to_file = "data/orders.csv"
    if not os.path.exists(path_to_file):
        print("❌ Orders file not found. Please run data_generator.py first.")
        exit(1)
    
    df = pd.read_csv(path_to_file)
    stream_order_events(df, delay=0.5)
