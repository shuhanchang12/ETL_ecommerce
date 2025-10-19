
# ETL-Warehousing Project Summary

## Directory Structure

```
ETL_github_v2/
├── src/
│   ├── data_generator.py          # Data generation
│   ├── producer_example.py        # Kafka producer
│   ├── consumer_to_snowflake.py   # Kafka consumer writes to Snowflake
│   ├── monitoring_app.py          # Streamlit monitoring dashboard
│   ├── snowflake_automation.sql   # Snowflake automation SQL
│   └── other helper scripts
├── data/
│   ├── customers.csv
│   ├── products.csv
│   └── orders.csv
├── .env                           # Kafka/Snowflake configuration
├── requirements.txt               # Python packages
├── docker-compose.yml             # Kafka/Redpanda Docker setup
├── setup_snowflake.sql, create_table.sql # Snowflake table creation SQL
├── setup_complete.sh, run_all_steps.sh   # One-click execution scripts
```

## Steps to Run

1. Install Python packages
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  ```
2. Configure the `.env` file
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

## Troubleshooting

- Kafka unreachable: Make sure Docker is running and port 9092 is free
- Snowflake permission error: Check .env credentials and role
- Dashboard shows no data: Make sure producer/consumer are running and Snowflake has data

---
For more details, SQL examples, or automation scripts, see the notebook documentation.
