import streamlit as st
import pandas as pd
import numpy as np
import time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from connection import get_db_connection

# ==========================================
# 1. PAGE CONFIGURATION
# ==========================================
st.set_page_config(page_title="Query Stack Dashboard", layout="wide")

# ==========================================
# 2. DATABASE CORE FUNCTIONS
# ==========================================
def fetch_ohlcv_data(symbol, table_name, start_time, end_time):
    """Requirement 1: Time Range Query Logic"""
    conn = get_db_connection()
    try:
        # Aligned with ts_bucket and volume from opt_tables.sql
        query = f"""
            SELECT ts_bucket, open_price, high_price, low_price, close_price, volume
            FROM {table_name}
            WHERE symbol = %s AND ts_bucket >= %s AND ts_bucket <= %s
            ORDER BY ts_bucket DESC;
        """
        df = pd.read_sql_query(query, conn, params=(symbol, start_time, end_time))
        for col in ['open_price', 'high_price', 'low_price', 'close_price']:
            df[col] = df[col].astype(float)
        return df
    finally:
        conn.close()

def run_live_benchmark(symbol, start_time, end_time):
    """Benchmarking: Standard DB vs. Optimized Aggregates"""
    conn = get_db_connection()
    cursor = conn.cursor()
    metrics = []
    
    # Standard DB Query (Requirement 4 on Raw Data)
    raw_q = "SELECT SUM(volume), COUNT(*) FROM raw_ticks WHERE symbol = %s AND ts >= %s AND ts <= %s;"
    t0 = time.perf_counter()
    cursor.execute(raw_q, (symbol, start_time, end_time))
    raw_res = cursor.fetchone()
    raw_latency = (time.perf_counter() - t0) * 1000 
    
    metrics.append({
        "Architecture": "Standard DB (Raw Ticks)",
        "Latency (ms)": round(raw_latency, 3),
        "Rows Processed": raw_res[1] if raw_res[1] else 0
    })

    # Optimized DB Query (Requirement 4 on Aggregated Data)
    opt_q = "SELECT SUM(volume), COUNT(*) FROM ohlcv_1m WHERE symbol = %s AND ts_bucket >= %s AND ts_bucket <= %s;"
    t1 = time.perf_counter()
    cursor.execute(opt_q, (symbol, start_time, end_time))
    opt_res = cursor.fetchone()
    opt_latency = (time.perf_counter() - t1) * 1000
    
    metrics.append({
        "Architecture": "Optimized (Aggregates)",
        "Latency (ms)": round(opt_latency, 3),
        "Rows Processed": opt_res[1] if opt_res[1] else 0
    })
    
    cursor.close()
    conn.close()
    return pd.DataFrame(metrics).set_index("Architecture")

def fetch_storage_stats():
    """Correctly calculates size of partitioned tables"""
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                parent.relname AS "Table",
                pg_size_pretty(SUM(pg_total_relation_size(child.oid))) AS "Size",
                SUM(pg_total_relation_size(child.oid)) AS "Bytes"
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child ON pg_inherits.inhrelid = child.oid
            WHERE parent.relname IN ('raw_ticks', 'ohlcv_1m')
            GROUP BY parent.relname;
        """
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()

# ==========================================
# 3. SIDEBAR & NAVIGATION
# ==========================================
with st.sidebar:
    st.title("Controls")
    selected_symbol = st.selectbox("Symbol", ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC'])
    
    st.subheader("Time Interval Selection")
    start_dt = datetime.combine(st.date_input("Start"), st.time_input("Start Time", datetime.now()-timedelta(hours=1)))
    end_dt = datetime.combine(st.date_input("End"), st.time_input("End Time", datetime.now()+timedelta(minutes=5)))
    
    res = st.radio("Interval Resolution", ["1-Sec", "1-Min"])
    table = "ohlcv_1s" if res == "1-Sec" else "ohlcv_1m"
    
    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()

# ==========================================
# 4. MAIN LAYOUT
# ==========================================
st.title(" Query Stack: Temporal Database Prototype")
tab_market, tab_bench = st.tabs([" Market Analysis", " Optimization Benchmarks"])

# ------------------------------------------
# TAB 1: MARKET ANALYSIS (Requirements 1-6)
# ------------------------------------------
with tab_market:
    df = fetch_ohlcv_data(selected_symbol, table, start_dt, end_dt)
    
    if df.empty:
        st.warning("No data found. Ensure the generator and aggregator are running.")
    else:
        # Data Calculations for Requirements
        df_sorted = df.sort_values('ts_bucket')
        
        # Requirement 5: Latest Price
        latest_price = df.iloc[0]['close_price']
        
        # Requirement 4: Volume Analysis
        total_volume = df['volume'].sum()
        
        # Requirement 6: Price Volatility (Std Dev of Close Price)
        volatility = df['close_price'].std()
        
        # Requirement 3: Moving Average (20-period SMA)
        df_sorted['sma_20'] = df_sorted['close_price'].rolling(window=20).mean()
        curr_sma = df_sorted['sma_20'].iloc[-1] if not df_sorted['sma_20'].isnull().all() else 0

        # --- Display Requirement Metrics ---
        st.subheader("Strategic Performance Indicators")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Latest Price", f"${latest_price:.2f}")
        c2.metric("Total Volume", f"{total_volume:,}")
        c3.metric("Price Volatility ", f"{volatility:.4f}")
        c4.metric("Moving Average", f"${curr_sma:.2f}")

        st.divider()

        # --- Requirement 2: OHLC Aggregation (Visual Chart) ---
        st.subheader("Financial Charting")
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05, row_width=[0.3, 0.7])
        
        # Candlestick View
        fig.add_trace(go.Candlestick(
            x=df_sorted['ts_bucket'], open=df_sorted['open_price'], high=df_sorted['high_price'], 
            low=df_sorted['low_price'], close=df_sorted['close_price'], name='OHLC'
        ), row=1, col=1)
        
        # SMA Overlay (Requirement 3)
        fig.add_trace(go.Scatter(
            x=df_sorted['ts_bucket'], y=df_sorted['sma_20'], line=dict(color='orange', width=2), name='20-SMA'
        ), row=1, col=1)

        # Volume Bar (Requirement 4)
        fig.add_trace(go.Bar(
            x=df_sorted['ts_bucket'], y=df_sorted['volume'], name='Volume', marker_color='#26A69A'
        ), row=2, col=1)

        fig.update_layout(height=600, xaxis_rangeslider_visible=False, margin=dict(t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

        # --- Requirement 1: Time Range Query (Tabular Results) ---
        st.subheader("Time Range Query Results")
        st.dataframe(df, use_container_width=True)

# ------------------------------------------
# TAB 2: BENCHMARKS (Latency + Storage)
# ------------------------------------------
with tab_bench:
    st.header("System Performance Benchmarks")
    
    # Latency Benchmark
    st.subheader("1. Query Latency Analysis")
    if st.button("Run Live Analytics Benchmark", type="primary"):
        bench_df = run_live_benchmark(selected_symbol, start_dt, end_dt)
        st.table(bench_df)
        
        raw_l = bench_df.loc["Standard DB (Raw Ticks)", "Latency (ms)"]
        opt_l = bench_df.loc["Optimized (Aggregates)", "Latency (ms)"]
        if opt_l > 0:
            st.success(f"**Optimization Result:** Aggregate queries are **{raw_l/opt_l:.1f}x faster** than standard scans.")

    st.divider()

    # Storage Statistics
    st.subheader("2. Storage Optimization Proof")
    stats = fetch_storage_stats()
    if not stats.empty:
        st.table(stats[['Table', 'Size']])
        
        raw_b = stats[stats['Table'] == 'raw_ticks']['Bytes'].values[0]
        opt_b = stats[stats['Table'] == 'ohlcv_1m']['Bytes'].values[0]
        if raw_b > 0:
            st.info(f"Aggregated blocks consume only **{(opt_b/raw_b)*100:.4f}%** of the total raw data footprint.")