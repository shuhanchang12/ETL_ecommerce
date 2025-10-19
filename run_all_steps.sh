#!/bin/bash

echo "🚀 Running Complete ETL Pipeline"

# Step 1: Create raw data and upload to database
echo "📊 Step 1: Creating raw data..."
python src/database_setup.py

# Step 2: Upload to Snowflake
echo "☁️ Uploading to Snowflake..."
python src/snowflake_upload.py

# Step 3: Start Kafka services
echo "🔄 Starting Kafka services..."
docker-compose up -d

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka..."
sleep 15

# Step 4: Start consumer
echo "📥 Starting consumer..."
python src/consumer_to_snowflake.py &
CONSUMER_PID=$!

# Step 5: Start producer
echo "📤 Starting producer..."
python src/producer_example.py &
PRODUCER_PID=$!

# Wait for events to be processed
echo "⏳ Processing events..."
sleep 30

# Stop processes
echo "🛑 Stopping processes..."
kill $CONSUMER_PID $PRODUCER_PID

# Step 6: Start monitoring app
echo "📊 Starting monitoring app..."
streamlit run src/monitoring_app.py

echo "✅ All steps completed!"
