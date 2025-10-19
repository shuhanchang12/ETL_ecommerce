
import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

# 讀取 .env 檔案中的 Snowflake 連線資訊
def get_snowflake_conn():
    load_dotenv()
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )

def upload_csv_to_snowflake(csv_path, table_name, conn):
    df = pd.read_csv(csv_path)
    cs = conn.cursor()
    try:
        for i, row in df.iterrows():
            columns = ','.join(df.columns)
            values = ','.join([f"'{str(x).replace("'", "''")}'" if pd.notnull(x) else 'NULL' for x in row])
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
            cs.execute(sql)
    finally:
        cs.close()

if __name__ == "__main__":
    # 你可以根據 Snowflake 的表名修改這裡
    csv_table_map = {
        'data/customers.csv': 'CUSTOMERS',
        'data/inventory_items.csv': 'INVENTORY_ITEMS',
        'data/orders.csv': 'ORDERS',
        'data/products.csv': 'PRODUCTS',
    }
    conn = get_snowflake_conn()
    for csv_path, table_name in csv_table_map.items():
        print(f"Uploading {csv_path} to {table_name} ...")
        upload_csv_to_snowflake(csv_path, table_name, conn)
    conn.close()
    print("All CSV files uploaded to Snowflake!")
