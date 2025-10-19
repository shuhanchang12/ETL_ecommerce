"""simple_consumer.py
Simple Kafka consumer using confluent-kafka
"""

import json
import os
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

# Configuration
BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "order_status_event")

# Create consumer
consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe([TOPIC_NAME])

print(f"Consuming messages from topic '{TOPIC_NAME}'...")
print("Press Ctrl+C to stop")

try:
    while True:
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
            
        # Parse the message
        event = json.loads(msg.value().decode('utf-8'))
        print(f"📨 Received: Order {event['order_id']} -> {event['new_status']} at {event['status_ts']}")
        
except KeyboardInterrupt:
    print("\n🛑 Stopping consumer...")
finally:
    consumer.close()
    print("✅ Consumer stopped")
