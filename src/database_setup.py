"""database_setup.py
Step 1: Create raw data and upload to SQL database
"""

import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
from data_generator import generate_customers, generate_inventory_data, generate_orders

load_dotenv()


def create_database_connection():
    """Create connection to PostgreSQL database."""
    try:
        engine = create_engine(
            f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        )
        return engine
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None


def create_tables(engine):
    """Create tables in the database."""
    print("Creating database tables...")
    
    # Create customers table
    customers_sql = """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        email VARCHAR(255),
        city VARCHAR(255),
        channel VARCHAR(50),
        created_at TIMESTAMP
    )
    """
    
    # Create products table
    products_sql = """
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR(255),
        category VARCHAR(100),
        unit_price DECIMAL(10,2),
        stock_quantity INTEGER
    )
    """
    
    # Create orders table
    orders_sql = """
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        product_id INTEGER,
        customer_id INTEGER,
        quantity INTEGER,
        unit_price DECIMAL(10,2),
        order_total DECIMAL(12,2),
        sold_at TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(product_id),
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    )
    """
    
    with engine.connect() as conn:
        conn.execute(customers_sql)
        conn.execute(products_sql)
        conn.execute(orders_sql)
        conn.commit()
    
    print("✅ Database tables created successfully")


def upload_data_to_database(engine):
    """Upload generated data to the database."""
    print("Uploading data to database...")
    
    # Generate data
    customers_df = generate_customers(customers=100, seed=42)
    products_df = generate_inventory_data(products=100, seed=42)
    orders_df = generate_orders(orders=100, customers_df=customers_df, products_df=products_df, seed=42)
    
    # Upload to database
    customers_df.to_sql('customers', engine, if_exists='replace', index=False)
    products_df.to_sql('products', engine, if_exists='replace', index=False)
    orders_df.to_sql('orders', engine, if_exists='replace', index=False)
    
    print("✅ Data uploaded to database successfully")


def extract_data_from_database(engine):
    """Extract data from database for Snowflake upload."""
    print("Extracting data from database...")
    
    customers_df = pd.read_sql("SELECT * FROM customers", engine)
    products_df = pd.read_sql("SELECT * FROM products", engine)
    orders_df = pd.read_sql("SELECT * FROM orders", engine)
    
    print(f"✅ Extracted {len(customers_df)} customers, {len(products_df)} products, {len(orders_df)} orders")
    
    return customers_df, products_df, orders_df


def main():
    """Main function for Step 1."""
    print("🚀 Step 1: Creating raw data and uploading to SQL database")
    
    # Create database connection
    engine = create_database_connection()
    if not engine:
        print("⚠️ Skipping database operations - using CSV files instead")
        # Generate data and save to CSV files
        customers_df = generate_customers(customers=100, seed=42)
        products_df = generate_inventory_data(products=100, seed=42)
        orders_df = generate_orders(orders=100, customers_df=customers_df, products_df=products_df, seed=42)
        
        # Save to CSV files
        customers_df.to_csv("data/customers_extracted.csv", index=False)
        products_df.to_csv("data/products_extracted.csv", index=False)
        orders_df.to_csv("data/orders_extracted.csv", index=False)
        
        print("✅ Data saved to CSV files for Snowflake upload")
        return
    
    try:
        # Create tables
        create_tables(engine)
        
        # Upload data
        upload_data_to_database(engine)
        
        # Extract data for next step
        customers_df, products_df, orders_df = extract_data_from_database(engine)
        
        # Save extracted data for Snowflake upload
        customers_df.to_csv("data/customers_extracted.csv", index=False)
        products_df.to_csv("data/products_extracted.csv", index=False)
        orders_df.to_csv("data/orders_extracted.csv", index=False)
        
        print("✅ Step 1 completed successfully!")
        
    except Exception as e:
        print(f"❌ Step 1 failed: {e}")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()



