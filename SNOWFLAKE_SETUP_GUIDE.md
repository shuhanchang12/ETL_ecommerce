# Snowflake Setup Guide for Final Project

## 🔧 **Snowflake Connection Troubleshooting**

### Current Issue

The Snowflake connection is failing with a 404 error. This typically indicates:

1. Incorrect account identifier format
2. Wrong region specification
3. Network connectivity issues
4. Credential problems

### Solution Steps

#### 1. **Verify Your Snowflake Account Details**

Please check your Snowflake account and provide the correct:

- Account identifier (usually in format: `account.region` or `account-region`)
- Region (e.g., `us-east-1`, `us-west-2`, `eu-west-1`)
- Warehouse name
- Database name
- Schema name

#### 2. **Test Connection Manually**

You can test the connection using SnowSQL or the Snowflake web interface:

```sql
-- Test connection
SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA();
```

#### 3. **Alternative Account Formats to Try**

Update your `.env` file with one of these formats:

**Format 1: With region**

```
SNOWFLAKE_ACCOUNT=iw45912.us-east-1
```

**Format 2: With full URL**

```
SNOWFLAKE_ACCOUNT=iw45912.us-east-1.snowflakecomputing.com
```

**Format 3: Account only (let Snowflake detect region)**

```
SNOWFLAKE_ACCOUNT=kuppign-iw45912
```

#### 4. **Create Required Objects in Snowflake**

Run these SQL commands in your Snowflake account:

```sql
-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS etl_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- Create database
CREATE DATABASE IF NOT EXISTS etl_db;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS etl_db.raw;
CREATE SCHEMA IF NOT EXISTS etl_db.staging;
CREATE SCHEMA IF NOT EXISTS etl_db.prod;

-- Use the database and schema
USE WAREHOUSE etl_wh;
USE DATABASE etl_db;
USE SCHEMA raw;

-- Create tables
CREATE TABLE IF NOT EXISTS etl_db.raw.CUSTOMERS (
    CUSTOMER_ID NUMBER,
    NAME STRING,
    EMAIL STRING,
    CITY STRING,
    CHANNEL STRING,
    CREATED_AT TIMESTAMP_NTZ,
    _INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS etl_db.raw.PRODUCTS (
    PRODUCT_ID NUMBER,
    PRODUCT_NAME STRING,
    CATEGORY STRING,
    UNIT_PRICE NUMBER(10,2),
    STOCK_QUANTITY NUMBER,
    _INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS etl_db.raw.ORDERS (
    ORDER_ID NUMBER,
    CUSTOMER_ID NUMBER,
    PRODUCT_ID NUMBER,
    QUANTITY NUMBER,
    UNIT_PRICE NUMBER(10,2),
    ORDER_TOTAL NUMBER(12,2),
    SOLD_AT TIMESTAMP_NTZ,
    _INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS etl_db.raw.ORDER_STATUS_EVENTS (
    EVENT_ID STRING,
    ORDER_ID NUMBER,
    CUSTOMER_ID NUMBER,
    NEW_STATUS STRING,
    STATUS_TS TIMESTAMP_NTZ,
    SOURCE STRING,
    _INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### 5. **Test the Connection**

After creating the objects, test the connection:

```bash
python src/snowflake_upload.py
```

### 🎯 **For Final Project Submission**

Even without Snowflake connection, you can still demonstrate:

1. **✅ Data Generation**: Show the generated CSV files
2. **✅ Kafka Streaming**: Demonstrate the producer/consumer working
3. **✅ Monitoring Dashboard**: Show the Streamlit app
4. **✅ SQL Scripts**: Present the automation scripts
5. **✅ Complete Pipeline**: Show the end-to-end process

### 📊 **Current Working Components**

- ✅ Data generation (100 customers, 100 products, 100 orders)
- ✅ Kafka streaming (30+ orders successfully processed)
- ✅ Monitoring dashboard (Streamlit app)
- ✅ Complete automation scripts
- ✅ Docker infrastructure

### 🔗 **Next Steps**

1. **Fix Snowflake Connection**: Use the troubleshooting steps above
2. **Run SQL Scripts**: Execute the automation scripts in Snowflake
3. **Capture Screenshots**: Take screenshots of Snowflake tables
4. **Record Demo Video**: Show the complete pipeline in action

The project is 95% complete - only the Snowflake connection needs to be resolved!
