#!/bin/bash

echo "🚀 Complete ETL Project Setup"

# Create .env file
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cat > .env << 'EOF'
# Kafka Configuration
KAFKA_BOOTSTRAP=localhost:19092
KAFKA_TOPIC_NAME=order_status_event

# Snowflake Configuration (Replace with your actual credentials)
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=TEACH_WH
SNOWFLAKE_DATABASE=RETAIL_LAB
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=retail_db
DB_USER=postgres
DB_PASSWORD=password
EOF
    echo "✅ .env file created. Please update with your credentials."
fi

# Create data directory
mkdir -p data

# Install dependencies
echo "📦 Installing dependencies..."
pip install -r requirements.txt

# Generate sample data
echo "📊 Generating sample data..."
python src/data_generator.py

# Build Docker image
echo "🐳 Building Docker image..."
docker build -t kafka-lab:latest .

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Update .env file with your Snowflake credentials"
echo "2. Run: docker-compose up -d"
echo "3. Run Step 1: python src/database_setup.py"
echo "4. Run Step 2: python src/producer_example.py & python src/consumer_to_snowflake.py"
echo "5. Run Step 3: Execute src/snowflake_automation.sql in Snowflake"
echo "6. Run Step 4: streamlit run src/monitoring_app.py"
