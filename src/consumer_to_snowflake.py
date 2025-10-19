"""consumer_to_snowflake.py
Step 2: Consume messages and send to Snowflake
"""

import json
import os
import time
from collections import deque

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from kafka import KafkaConsumer
from snowflake.connector.pandas_tools import write_pandas

# Configuration
BATCH_SIZE = 50
BATCH_SECONDS = 10

load_dotenv()
TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "order_status_event")

# Initialize consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="snowflake_loader",
)

# Initialize Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)


def create_event_table(conn):
    """Create the event table in Snowflake."""
    print("Creating event table...")
    sql = """
    CREATE TABLE IF NOT EXISTS RETAIL_LAB.RAW.ORDER_STATUS_EVENTS (
        EVENT_ID STRING,
        ORDER_ID NUMBER,
        CUSTOMER_ID NUMBER,
        NEW_STATUS STRING,
        STATUS_TS TIMESTAMP_NTZ,
        SOURCE STRING,
        _INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        print("✅ Event table created")
    finally:
        cur.close()


def flush_batch(conn, buffer):
    """Flush the current buffer to Snowflake."""
    if not buffer:
        return
    
    df = pd.DataFrame(buffer)
    write_pandas(
        conn,
        df,
        "ORDER_STATUS_EVENTS",
        database="RETAIL_LAB",
        schema="RAW",
        auto_create_table=False,
        overwrite=False,
    )
    print(f"✅ Inserted {len(df)} events into Snowflake")
    buffer.clear()


if __name__ == "__main__":
    # Create event table
    create_event_table(conn)
    
    buffer = deque()
    last_flush = time.time()
    print("Consuming events and writing to Snowflake... Ctrl+C to stop.")

    try:
        for msg in consumer:
            evt = msg.value
            buffer.append({
                "EVENT_ID": evt["event_id"],
                "ORDER_ID": evt["order_id"],
                "CUSTOMER_ID": evt["customer_id"],
                "NEW_STATUS": evt["new_status"],
                "STATUS_TS": evt["status_ts"],
                "SOURCE": evt.get("source", "order_service"),
            })

            if len(buffer) >= BATCH_SIZE or (time.time() - last_flush) >= BATCH_SECONDS:
                flush_batch(conn, buffer)
                last_flush = time.time()
                
    except KeyboardInterrupt:
        print("\n🛑 Stopping consumer...")
    finally:
        flush_batch(conn, buffer)
        conn.close()
        print("✅ Consumer stopped")
