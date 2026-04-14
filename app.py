import streamlit as st
import pandas as pd
import numpy as np
import time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from utils.connection import get_db_connection
from streamlit_autorefresh import st_autorefresh  

st.set_page_config(page_title="Query Stack Dashboard", layout="wide")

def detect_architecture(cursor):
    cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'raw_ticks_bucketed');")
    if cursor.fetchone()[0]:
        return "Compressed Storage (LZ4 Arrays)", True
    
    cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'raw_ticks');")
    if cursor.fetchone()[0]:
        cursor.execute("SELECT indexname FROM pg_indexes WHERE tablename = 'raw_ticks' AND indexname = 'idx_raw_ticks_ts';")
        if cursor.fetchone():
            return "Indexed Storage (B-Tree)", False
        return "Baseline Storage (No Indexes)", False
        
    return "Unknown Architecture", False

def fetch_ohlcv_data(symbol, table_name, start_time, end_time):
    conn = get_db_connection()
    try:
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
    conn = get_db_connection()
    cursor = conn.cursor()
    metrics = []
    
    arch_name, is_compressed = detect_architecture(cursor)

    queries = [
        {
            "name": "Q1: Volume Aggregate (I/O Bound)",
            "std": "SELECT SUM(volume), COUNT(*) FROM raw_ticks WHERE symbol = %s AND ts >= %s AND ts <= %s;",
            "arr": "SELECT SUM((SELECT SUM(v) FROM unnest(volumes) v)), COUNT(*) FROM raw_ticks_bucketed WHERE symbol = %s AND bucket_ts >= %s AND bucket_ts <= %s;"
        },
        {
            "name": "Q2: VWAP Calculation (CPU Bound)",
            "std": "SELECT SUM(price * volume) / NULLIF(SUM(volume), 0) FROM raw_ticks WHERE symbol = %s AND ts >= %s AND ts <= %s;",
            "arr": "SELECT SUM((SELECT SUM(p*v) FROM unnest(prices, volumes) AS t(p,v))) / NULLIF(SUM((SELECT SUM(v) FROM unnest(volumes) v)), 0) FROM raw_ticks_bucketed WHERE symbol = %s AND bucket_ts >= %s AND bucket_ts <= %s;"
        },
        {
            "name": "Q3: Minute Rollup (Memory/Sort Bound)",
            "std": "SELECT date_trunc('minute', ts), SUM(volume) FROM raw_ticks WHERE symbol = %s AND ts >= %s AND ts <= %s GROUP BY 1 ORDER BY 1;",
            "arr": "SELECT bucket_ts, SUM((SELECT SUM(v) FROM unnest(volumes) v)) FROM raw_ticks_bucketed WHERE symbol = %s AND bucket_ts >= %s AND bucket_ts <= %s GROUP BY 1 ORDER BY 1;"
        }
    ]

    executed_sqls = []
    
    for q in queries:
        t0 = time.perf_counter()
        sql = q["arr"] if is_compressed else q["std"]
        cursor.execute(sql, (symbol, start_time, end_time))
        cursor.fetchall()
        latency = (time.perf_counter() - t0) * 1000
        
        metrics.append({
            "Query Type": q["name"],
            "Target Engine": arch_name,
            "Latency (ms)": round(latency, 3)
        })
        executed_sqls.append(f" {q['name']}\n{sql}")
    
    cursor.close()
    conn.close()
    
    display_sql = "\n\n".join(executed_sqls).replace("%s", f"'{symbol}'", 1).replace("%s", f"'{start_time}'", 1).replace("%s", f"'{end_time}'", 1)
    
    return pd.DataFrame(metrics), display_sql

def fetch_storage_stats():
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
            WHERE parent.relname IN ('raw_ticks', 'raw_ticks_bucketed', 'ohlcv_1m', 'ohlcv_1s')
            GROUP BY parent.relname;
        """
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning) 
            return pd.read_sql_query(query, conn)
    finally:
        conn.close()

with st.sidebar:
    st.title("Controls")
    selected_symbol = st.selectbox("Symbol", ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC'])
    
    st.subheader("Time Interval Selection")
    default_date = datetime(2024, 4, 11).date()

    c1, c2 = st.columns(2)
    start_time_str = c1.text_input("Start Time", value="14:00:00")
    end_time_str = c2.text_input("End Time", value="14:15:00")

    try:
        start_time_obj = datetime.strptime(start_time_str, "%H:%M:%S").time()
        end_time_obj = datetime.strptime(end_time_str, "%H:%M:%S").time()
    except ValueError:
        st.error("⚠️ Format HH:MM:SS")
        start_time_obj = datetime(2024, 4, 11, 14, 0).time()
        end_time_obj = datetime(2024, 4, 11, 14, 15).time()

    start_dt = datetime.combine(st.date_input("Start Date", value=default_date), start_time_obj)
    end_dt = datetime.combine(st.date_input("End Date", value=default_date), end_time_obj)

    res = st.radio("Interval Resolution", ["1-Sec", "1-Min"])
    table = "ohlcv_1s" if res == "1-Sec" else "ohlcv_1m"
    
    st.divider()
    auto_refresh = st.toggle("🔴 Live Auto-Refresh", value=False)
    if auto_refresh:
        st_autorefresh(interval=2000, limit=None, key="live_refresh")
    if st.button("Refresh Manually", use_container_width=True):
        st.cache_data.clear()

st.title("📈 Query Stack: Temporal Database")
tab_market, tab_bench = st.tabs(["📊 Market Analysis", "⚡ Optimization Benchmarks"])

with tab_market:
    df = fetch_ohlcv_data(selected_symbol, table, start_dt, end_dt)
    if not df.empty:
        df_sorted = df.sort_values('ts_bucket')
        
        latest_price = df.iloc[0]['close_price']
        total_volume = df['volume'].sum()
        volatility = df['close_price'].std()
        df_sorted['sma_20'] = df_sorted['close_price'].rolling(window=20).mean()
        curr_sma = df_sorted['sma_20'].iloc[-1] if not df_sorted['sma_20'].isnull().all() else 0

        st.subheader("Strategic Performance Indicators")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Latest Price", f"${latest_price:.2f}")
        col2.metric("Total Volume", f"{total_volume:,}")
        col3.metric("Price Volatility", f"{volatility:.4f}")
        col4.metric("Moving Average", f"${curr_sma:.2f}")
        st.divider()

        st.subheader("Financial Charting")
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05, row_width=[0.3, 0.7])
        
        fig.add_trace(go.Candlestick(x=df_sorted['ts_bucket'], open=df_sorted['open_price'], high=df_sorted['high_price'], low=df_sorted['low_price'], close=df_sorted['close_price'], name='OHLC'), row=1, col=1)
        
        fig.add_trace(go.Scatter(x=df_sorted['ts_bucket'], y=df_sorted['sma_20'], line=dict(color='orange', width=2), name='20-SMA'), row=1, col=1)
        
        fig.add_trace(go.Bar(x=df_sorted['ts_bucket'], y=df_sorted['volume'], name='Volume', marker_color='#26A69A'), row=2, col=1)
        
        fig.update_layout(height=600, xaxis_rangeslider_visible=False, margin=dict(t=0, b=0), uirevision='constant')
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Time Range Query Results")
        st.dataframe(df, use_container_width=True, height=400)
    else:
        st.warning("No data found. Ensure the ingester is running.")

with tab_bench:
    st.header("System Performance Benchmarks")
    
    if "bench_df" not in st.session_state:
        st.session_state.bench_df = None

    st.subheader("1. Multi-Query Workload Analysis")
    st.markdown("Executes a set of queries testing I/O limits, CPU math processing, and Memory sorting.")
    
    if st.button("Run queries", type="primary"):
        with st.spinner("Executing query suite..."):
            bench_df, suite_sql = run_live_benchmark(selected_symbol, start_dt, end_dt)
            st.session_state.bench_df = bench_df
            st.session_state.suite_sql = suite_sql

    if st.session_state.bench_df is not None:
        df_pivot = st.session_state.bench_df.pivot(index="Query Type", columns="Target Engine", values="Latency (ms)").fillna("-")
        st.dataframe(df_pivot, use_container_width=True)

        with st.expander("🔍 View Executed Benchmark SQL"):
            st.code(st.session_state.suite_sql, language="sql")

    st.divider()

    st.subheader("2. Storage Profile")
    stats = fetch_storage_stats()
    if not stats.empty:
        display_stats = stats[['Table', 'Size', 'Bytes']].sort_values(by="Bytes", ascending=False)
        display_stats['Share of DB (%)'] = ((display_stats['Bytes'] / display_stats['Bytes'].sum()) * 100).round(2)
        st.dataframe(display_stats.drop(columns=['Bytes']), use_container_width=True)