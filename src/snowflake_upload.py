"""snowflake_upload.py
Upload data from database to Snowflake
"""

import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()


def create_snowflake_connection():
    """Create connection to Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
        return conn
    except Exception as e:
        print(f"❌ Snowflake connection failed: {e}")
        return None


def create_snowflake_tables(conn):
    """Create tables in Snowflake."""
    print("Creating Snowflake tables...")
    
    tables = {
        "CUSTOMERS": """
        CREATE TABLE IF NOT EXISTS RETAIL_LAB.RAW.CUSTOMERS (
            CUSTOMER_ID NUMBER,
            NAME STRING,
            EMAIL STRING,
            CITY STRING,
            CHANNEL STRING,
            CREATED_AT TIMESTAMP_NTZ,
            _INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        "PRODUCTS": """
        CREATE TABLE IF NOT EXISTS RETAIL_LAB.RAW.PRODUCTS (
            PRODUCT_ID NUMBER,
            PRODUCT_NAME STRING,
            CATEGORY STRING,
            UNIT_PRICE NUMBER(10,2),
            STOCK_QUANTITY NUMBER,
            _INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """,
        "ORDERS": """
        CREATE TABLE IF NOT EXISTS RETAIL_LAB.RAW.ORDERS (
            ORDER_ID NUMBER,
            CUSTOMER_ID NUMBER,
            PRODUCT_ID NUMBER,
            QUANTITY NUMBER,
            UNIT_PRICE NUMBER(10,2),
            ORDER_TOTAL NUMBER(12,2),
            SOLD_AT TIMESTAMP_NTZ,
            _INGESTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    }
    
    cur = conn.cursor()
    try:
        for table_name, sql in tables.items():
            cur.execute(sql)
            print(f"✅ Created table {table_name}")
    finally:
        cur.close()


def upload_data_to_snowflake(conn):
    """Upload data to Snowflake."""
    print("Uploading data to Snowflake...")
    
    # Load data
    customers_df = pd.read_csv("data/customers_extracted.csv")
    products_df = pd.read_csv("data/products_extracted.csv")
    orders_df = pd.read_csv("data/orders_extracted.csv")
    
    # Upload to Snowflake
    write_pandas(conn, customers_df, "CUSTOMERS", database="RETAIL_LAB", schema="RAW", auto_create_table=False, overwrite=True)
    write_pandas(conn, products_df, "PRODUCTS", database="RETAIL_LAB", schema="RAW", auto_create_table=False, overwrite=True)
    write_pandas(conn, orders_df, "ORDERS", database="RETAIL_LAB", schema="RAW", auto_create_table=False, overwrite=True)
    
    print("✅ Data uploaded to Snowflake successfully")


def main():
    """Main function for Snowflake upload."""
    print("🚀 Uploading data to Snowflake")
    
    # Create Snowflake connection
    conn = create_snowflake_connection()
    if not conn:
        print("⚠️ Skipping Snowflake upload - please check your credentials in .env file")
        return
    
    try:
        # Create tables
        create_snowflake_tables(conn)
        
        # Upload data
        upload_data_to_snowflake(conn)
        
        print("✅ Snowflake upload completed successfully!")
        
    except Exception as e:
        print(f"❌ Snowflake upload failed: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()


