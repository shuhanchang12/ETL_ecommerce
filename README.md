# ETL-Warehousing Project

## Slide:


<div style="position: relative; width: 100%; height: 0; padding-top: 56.2500%;
 padding-bottom: 0; box-shadow: 0 2px 8px 0 rgba(63,69,81,0.16); margin-top: 1.6em; margin-bottom: 0.9em; overflow: hidden;
 border-radius: 8px; will-change: transform;">
  <iframe loading="lazy" style="position: absolute; width: 100%; height: 100%; top: 0; left: 0; border: none; padding: 0;margin: 0;"
    src="https://www.canva.com/design/DAG2JrqxqKk/DMyxatgzu46sJ2j63Q2h7w/view?embed" allowfullscreen="allowfullscreen" allow="fullscreen">
  </iframe>
</div>
<a href="https://www.canva.com/design/DAG2JrqxqKk/DMyxatgzu46sJ2j63Q2h7w/view?utm_content=DAG2JrqxqKk&utm_campaign=designshare&utm_medium=embeds&utm_source=link" target="_blank" rel="noopener">Presentation - Cloud Data Pipeline</a> by Shuhan Chang


## Project Overview

This project demonstrates a complete ETL pipeline: data generation, event streaming (using Redpanda), data warehousing, and monitoring analytics. Suitable for teaching or prototyping.

## Directory Structure

- `src/`: All Python scripts
  - `data_generator.py`: Generate customers/orders/products CSV files
   - `producer_example.py`: Send order events to Redpanda
  - `consumer_to_snowflake.py`: Receive events and write to Snowflake
  - `monitoring_app.py`: Streamlit monitoring dashboard
  - Other helper scripts
- `data/`: Generated CSV files
- `.env`: Environment variables (Kafka/Snowflake credentials)
- `requirements.txt`: Python package requirements
- `docker-compose.yml`: Redpanda (Kafka-compatible) Docker setup
- `setup_snowflake.sql`, `create_table.sql`: Snowflake table creation SQL

## Quick Start

1. Install Python packages
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Configure the `.env` file (fill in credentials as shown in the example)
3. Start Redpanda
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

- Redpanda unreachable: Make sure Docker is running and port 9092 is free
- Snowflake permission error: Check .env credentials and role
- Dashboard shows no data: Make sure producer/consumer are running and Snowflake has data

## Completion Checklist

- [X] venv created and packages installed
- [X] .env configured
- [X] Redpanda running
- [X] Data CSVs generated
- [X] Snowflake RAW table created
- [X] Consumer script running
- [X] Producer script running
- [X] Data visible in Snowflake
- [X] Streamlit monitoring dashboard working

---

For more details, SQL examples, or automation scripts, see `PROJECT_SUMMARY.md` or the notebook documentation.
