"""monitoring_app.py
Step 4: Streamlit monitoring application
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()


def create_snowflake_connection():
    """Create connection to Snowflake."""
    try:
        return snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {e}")
        return None


def get_order_monitoring_data(conn):
    """Get order monitoring data from Snowflake."""
    query = """
    SELECT 
        NEW_STATUS as CURRENT_STATUS,
        COUNT(*) as ORDER_COUNT,
        COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS,
        MIN(STATUS_TS) as EARLIEST_UPDATE,
        MAX(STATUS_TS) as LATEST_UPDATE
    FROM RETAIL_LAB.RAW.ORDER_STATUS_EVENTS
    GROUP BY NEW_STATUS
    ORDER BY ORDER_COUNT DESC
    """
    return pd.read_sql(query, conn)


def get_daily_orders_data(conn):
    """Get daily orders data."""
    query = """
    SELECT 
        DATE(STATUS_TS) as ORDER_DATE,
        NEW_STATUS as CURRENT_STATUS,
        COUNT(*) as ORDER_COUNT,
        COUNT(DISTINCT CUSTOMER_ID) as UNIQUE_CUSTOMERS
    FROM RETAIL_LAB.RAW.ORDER_STATUS_EVENTS
    WHERE STATUS_TS >= CURRENT_DATE - 30
    GROUP BY ORDER_DATE, NEW_STATUS
    ORDER BY ORDER_DATE, CURRENT_STATUS
    """
    return pd.read_sql(query, conn)


def get_customer_analytics(conn):
    """Get customer analytics data."""
    query = """
    SELECT 
        CUSTOMER_ID,
        COUNT(*) as TOTAL_ORDERS,
        ARRAY_AGG(DISTINCT NEW_STATUS) as STATUS_TYPES,
        MIN(STATUS_TS) as FIRST_ORDER_DATE,
        MAX(STATUS_TS) as LAST_ORDER_DATE,
        MAX(CASE WHEN NEW_STATUS = 'DELIVERED' THEN 1 ELSE 0 END) as HAS_DELIVERED_ORDERS
    FROM RETAIL_LAB.RAW.ORDER_STATUS_EVENTS
    GROUP BY CUSTOMER_ID
    ORDER BY TOTAL_ORDERS DESC
    LIMIT 20
    """
    return pd.read_sql(query, conn)


def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="ETL Pipeline Monitoring",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 ETL Pipeline Monitoring Dashboard")
    st.markdown("Real-time monitoring of order processing pipeline")
    
    # Create connection
    conn = create_snowflake_connection()
    
    if conn is None:
        st.error("❌ Cannot connect to Snowflake. Please check your credentials in .env file")
        st.info("💡 You can still view the demo data below")
        
        # Demo data
        demo_monitoring = pd.DataFrame({
            'CURRENT_STATUS': ['CREATED', 'PAID', 'PACKED', 'SHIPPED', 'DELIVERED'],
            'ORDER_COUNT': [45, 38, 25, 18, 12],
            'UNIQUE_CUSTOMERS': [42, 35, 22, 16, 11]
        })
        
        demo_daily = pd.DataFrame({
            'ORDER_DATE': pd.date_range('2024-01-01', periods=7),
            'CURRENT_STATUS': ['CREATED'] * 7,
            'ORDER_COUNT': [5, 7, 6, 8, 9, 7, 6]
        })
        
        # Display demo data
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Orders", demo_monitoring['ORDER_COUNT'].sum())
        
        with col2:
            st.metric("Unique Customers", demo_monitoring['UNIQUE_CUSTOMERS'].sum())
        
        with col3:
            st.metric("Status Types", len(demo_monitoring))
        
        with col4:
            st.metric("Latest Update", "Demo Mode")
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Order Status Distribution (Demo)")
            fig = px.pie(demo_monitoring, values='ORDER_COUNT', names='CURRENT_STATUS', 
                        title="Orders by Status")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Daily Order Trends (Demo)")
            fig = px.line(demo_daily, x='ORDER_DATE', y='ORDER_COUNT', 
                        title="Daily Order Count")
            st.plotly_chart(fig, use_container_width=True)
        
        return
    
    try:
        # Get data
        monitoring_df = get_order_monitoring_data(conn)
        daily_df = get_daily_orders_data(conn)
        customer_df = get_customer_analytics(conn)
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Orders", monitoring_df['ORDER_COUNT'].sum())
        
        with col2:
            st.metric("Unique Customers", monitoring_df['UNIQUE_CUSTOMERS'].sum())
        
        with col3:
            st.metric("Status Types", len(monitoring_df))
        
        with col4:
            latest_update = monitoring_df['LATEST_UPDATE'].max()
            st.metric("Latest Update", latest_update.strftime('%H:%M:%S') if pd.notna(latest_update) else "N/A")
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Order Status Distribution")
            fig = px.pie(monitoring_df, values='ORDER_COUNT', names='CURRENT_STATUS', 
                        title="Orders by Status")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Daily Order Trends")
            if not daily_df.empty:
                fig = px.line(daily_df, x='ORDER_DATE', y='ORDER_COUNT', 
                            color='CURRENT_STATUS', title="Daily Order Count by Status")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No daily data available")
        
        # Data tables
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Order Status Details")
            st.dataframe(monitoring_df, use_container_width=True)
        
        with col2:
            st.subheader("Top Customers")
            st.dataframe(customer_df, use_container_width=True)
        
        # Raw data
        with st.expander("Raw Daily Data"):
            st.dataframe(daily_df, use_container_width=True)
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
