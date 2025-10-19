from snowflake.connector import connect
import os
from dotenv import load_dotenv

load_dotenv()
try:
    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    print("✅ Snowflake connection successful!")
    conn.close()
except Exception as e:
    print(f"❌ Snowflake connection failed: {e}")