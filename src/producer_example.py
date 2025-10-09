"""producer.py
A simple Kafka producer that generates and sends order events to a Kafka topic.
⚠️ update in your producer container the path to this file
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer

# ENVIRONMENT VARIABLES (optional, defaults provided)
# OPTIONAL: if you want to generate on the fly, import your generator
# from data_generator import generate_orders, generate_inventory_data, generate_customers

BOOTSTRAP_HOST = os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost")
BOOTSTRAP_PORT = os.getenv("KAFKA_BOOTSTRAP_PORT", 9092)
TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")  # SET your topic name in the .env file
BOOTSTRAP_SERVER = f"{BOOTSTRAP_HOST}:{BOOTSTRAP_PORT}"

# The serializer is used to convert the order to a JSON string
print(BOOTSTRAP_SERVER)
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))

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


# Create events based on existing orders
# Each order can have multiple events (status updates)
def event_from_order(order):
    """Map an orders row to a streaming interaction (simple mapping).
    For a given order, create a new event with a random status."""
    status = rng.choices([s for s, _ in statuses], weights=[w for _, w in statuses])[0]
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": order.get("id"),
        "customer_id": order.get("customer_id"),
        "new_status": status,
        "status_ts": datetime.now(timezone.utc).isoformat(),
        "source": "order_service",
    }


def stream_from_dataframe(df, delay=0.3):
    """
    For each order, produce a realistic sequence of status events (CREATED → PAID → ...).
    """
    print(f"Producing full status progressions for {len(df)} orders to topic '{TOPIC_NAME}' ... Ctrl+C to stop.")
    # Define a canonical status progression (no repeats, logical order)
    status_sequence = [s for s, _ in statuses if s not in ("CANCELLED", "REFUNDED", "RETURNED")]
    for _, row in df.iterrows():
        # Optionally, randomly decide how far the order gets in the lifecycle
        max_idx = random.randint(2, len(status_sequence))  # at least CREATED+PAID, up to DELIVERED
        # HERE an order can have multiple events/status
        for status in status_sequence[:max_idx]:
            evt = {
                "event_id": str(uuid.uuid4()),
                "order_id": row.get("id"),
                "customer_id": row.get("customer_id"),
                "new_status": status,
                "status_ts": datetime.now(timezone.utc).isoformat(),
                "source": "order_service",
            }
            producer.send(TOPIC_NAME, value=evt)
            print("→", evt)
            time.sleep(delay)

        # Optionally, sometimes emit a terminal event (CANCELLED, REFUNDED, RETURNED)
        if random.random() < 0.15:
            terminal_status = random.choice([s for s in ("CANCELLED", "REFUNDED", "RETURNED")])
            evt = {
                "event_id": str(uuid.uuid4()),
                "order_id": row.get("id"),
                "customer_id": row.get("customer_id"),
                "new_status": terminal_status,
                "status_ts": datetime.now(timezone.utc).isoformat(),
                "source": "order_service",
            }
            producer.send(TOPIC_NAME, value=evt)
            print("→", evt)
            time.sleep(delay)
    producer.flush()


if __name__ == "__main__":
    # I have pre-generated some orders data to avoid generating too much data on the fly and save them to a CSV file
    path_to_file = "data/orders.csv"  # ⚠️ update this path
    df = pd.read_csv(path_to_file)
    stream_from_dataframe(df, delay=0.3)
