# ETL-Warehousing


# Hairvana — ETL & Warehousing Demo

An end-to-end, classroom-friendly data project:

- Interactive, single-page dashboard explaining the pipeline, schemas, star model, and KPIs (page.html with Tailwind + Chart.js)
- Optional hands-on ETL demo with Python, Kafka (local via Docker), and Snowflake RAW loading

Use this to learn, demo, or prototype modern data workflows.

## Tech stack

- Data/ETL (optional): Python 3, Kafka, Snowflake, Docker Desktop (for local Kafka)

## Repository layout

- page.html — Interactive dashboard (no backend required)
- src/
  - data_generator.py — Creates sample CSVs (customers/orders/products)
  - producer_example.py — Reads CSV and streams order status events to Kafka
  - consumer_to_snowflake.py — Consumes Kafka events and batch-writes to Snowflake
  - pipeline_example.py — Optional batch example (if present)
- requirements.txt, Makefile, Dockerfile (if present)

## Quick start — dashboard only

No backend needed. From the repo root:

```bash
python -m http.server 8000
# Open http://localhost:8000/page.html
```

What the page shows (mapped to this project):

- Pipeline stages: Sources → 12h Batch Ingestion → Storage & ETL (GCS + Dataflow + Python) → Snowflake DWH → Visualization (Tableau/Looker)
- Data Structures tabs: ORDERS, CUSTOMERS, PRODUCTS
- Star schema: ORDERS fact table linked to CUSTOMERS and PRODUCTS dimensions
- KPIs: Switch business area and region; Chart.js updates dynamically

## Optional: end-to-end ETL demo

1) Clone and enter the project (if not already)

```bash
cd "/Users/shellychang/Library/CloudStorage/GoogleDrive-shuhc121@gmail.com/我的雲端硬碟/Albertschool_M2_1/as_m2-1/ETL and Warehousing"
git clone https://github.com/CazenaveAlbertSchool/ETL-Warehousing.git
cd "ETL-Warehousing"
```

2) Python env and dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# or: make install
```

3) Environment variables (.env in repo root)

```ini
# Kafka
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC_NAME=order_status_event

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RETAIL_LAB
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

4) Start Kafka with Docker (docker-compose.yml in repo root)

```yaml
version: "3.8"
services:
  zookeeper:
    image: bitnami/zookeeper:3.9
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
    ports:
      - "2181:2181"
  kafka:
    image: bitnami/kafka:3.6
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://:29092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9092
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true
      - ALLOW_PLAINTEXT_LISTENER=yes
```

```bash
docker compose up -d
nc -zv localhost 9092
```

5) Generate sample data (creates data/ CSVs)

```bash
python src/data_generator.py
```

6) Snowflake RAW table (Snowsight/SnowSQL)

```sql
CREATE OR REPLACE DATABASE RETAIL_LAB;
CREATE OR REPLACE SCHEMA RETAIL_LAB.RAW;

CREATE OR REPLACE TABLE RETAIL_LAB.RAW.ORDER_STATUS_EVENTS (
  EVENT_ID     STRING,
  ORDER_ID     STRING,
  CUSTOMER_ID  STRING,
  NEW_STATUS   STRING,
  STATUS_TS    TIMESTAMP_NTZ,
  SOURCE       STRING
);
```

7) Run the consumer (first)

```bash
python src/consumer_to_snowflake.py
```

8) Run the producer (send events from data/orders.csv)

```bash
python src/producer_example.py
```

9) Validate in Snowflake

```sql
SELECT COUNT(*) FROM RETAIL_LAB.RAW.ORDER_STATUS_EVENTS;
SELECT * FROM RETAIL_LAB.RAW.ORDER_STATUS_EVENTS LIMIT 20;
```

## Data models shown

- ORDERS (fact): ORDER_ID, CUSTOMER_ID, PRODUCT_ID, QUANTITY, ORDER_TOTAL, ORDER_STATUS, SOLD_AT, REGION
- CUSTOMERS (dim): CUSTOMER_ID, NAME, EMAIL, JOIN_DATE, ADDRESS, CITY, COUNTRY, POSTAL_CODE, REGION
- PRODUCTS (dim): PRODUCT_ID, PRODUCT_NAME, BRAND, CATEGORY, UNIT_PRICE, STOCK_QUANTITY

Edit these definitions directly in page.html to match your backend.

## Troubleshooting

- git not found (macOS): xcode-select --install
- Activate venv: source .venv/bin/activate
- Kafka unreachable: ensure Docker is running, port 9092 is free (nc -zv localhost 9092)
- Snowflake auth/permissions: check .env and role access to warehouse/db/schema
- Paths with spaces/Unicode: wrap in double quotes (as done above)

## Completion checklist

- [ ] Venv created and requirements installed
- [ ] .env configured (Kafka + Snowflake)
- [ ] Kafka up and reachable (localhost:9092)
- [ ] data/*.csv generated
- [ ] Snowflake RAW table created
- [ ] Consumer running without errors
- [ ] Producer sent events
- [ ] Rows visible in Snowflake
- [ ] Dashboard loads at http://localhost:8000/page.html
