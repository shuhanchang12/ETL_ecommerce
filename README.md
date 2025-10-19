
# ETL-Warehousing Project

## Project Overview
This project demonstrates a complete ETL pipeline: data generation, event streaming, data warehousing, and monitoring analytics. Suitable for teaching or prototyping.

## Directory Structure

- `src/`: All Python scripts
  - `data_generator.py`: Generate customers/orders/products CSV files
  - `producer_example.py`: Send order events to Kafka
  - `consumer_to_snowflake.py`: Receive events and write to Snowflake
  - `monitoring_app.py`: Streamlit monitoring dashboard
  - Other helper scripts
- `data/`: Generated CSV files
- `.env`: Environment variables (Kafka/Snowflake credentials)
- `requirements.txt`: Python package requirements
- `docker-compose.yml`: Kafka/Redpanda Docker setup
- `setup_snowflake.sql`, `create_table.sql`: Snowflake table creation SQL

## Quick Start

1. Install Python packages
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Configure the `.env` file (fill in credentials as shown in the example)
3. Start Kafka/Redpanda
   ```bash
   docker compose up -d
   nc -zv localhost 9092
   ```
4. Generate data
   ```bash
   python src/data_generator.py
   ```
5. Create Snowflake RAW table (run the SQL file contents)
6. Start the consumer
   ```bash
   python src/consumer_to_snowflake.py
   ```
7. Start the producer
   ```bash
   python src/producer_example.py
   ```
8. Query data in Snowflake
   ```sql
   SELECT COUNT(*) FROM RETAIL_LAB.RAW.ORDER_STATUS_EVENTS;
   SELECT * FROM RETAIL_LAB.RAW.ORDER_STATUS_EVENTS LIMIT 20;
   ```
9. Start the monitoring dashboard
   ```bash
   streamlit run src/monitoring_app.py
   ```

## Troubleshooting

- Kafka unreachable: Make sure Docker is running and port 9092 is free
- Snowflake permission error: Check .env credentials and role
- Dashboard shows no data: Make sure producer/consumer are running and Snowflake has data

## Completion Checklist

- [x] venv created and packages installed
- [x] .env configured
- [x] Kafka/Redpanda running
- [x] Data CSVs generated
- [x] Snowflake RAW table created
- [x] Consumer script running
- [x] Producer script running
- [x] Data visible in Snowflake
- [x] Streamlit monitoring dashboard working

---
For more details, SQL examples, or automation scripts, see `PROJECT_SUMMARY.md` or the notebook documentation.
