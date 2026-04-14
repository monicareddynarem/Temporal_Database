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
    """Benchmarking: Compressed Arrays vs. Optimized Aggregates"""
    conn = get_db_connection()
    cursor = conn.cursor()
    metrics = []
    
    cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'raw_ticks_bucketed');")
    is_compressed = cursor.fetchone()[0]

    t0 = time.perf_counter()
    if is_compressed:
        raw_q = """
            SELECT SUM((SELECT SUM(v) FROM unnest(volumes) v)), COUNT(*) 
            FROM raw_ticks_bucketed 
            WHERE symbol = %s AND bucket_ts >= %s AND bucket_ts <= %s;
        """
    else:
        raw_q = "SELECT SUM(volume), COUNT(*) FROM raw_ticks WHERE symbol = %s AND ts >= %s AND ts <= %s;"
        
    cursor.execute(raw_q, (symbol, start_time, end_time))
    raw_res = cursor.fetchone()
    raw_latency = (time.perf_counter() - t0) * 1000 
    
    metrics.append({
        "Architecture": "LZ4 Arrays (Unnested)" if is_compressed else "Standard DB (Raw Ticks)",
        "Latency (ms)": round(raw_latency, 3),
        "Rows Processed": raw_res[1] if raw_res[1] else 0
    })

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
    
    display_raw_q = raw_q.replace("%s", f"'{symbol}'", 1).replace("%s", f"'{start_time}'", 1).replace("%s", f"'{end_time}'", 1).strip()
    display_opt_q = opt_q.replace("%s", f"'{symbol}'", 1).replace("%s", f"'{start_time}'", 1).replace("%s", f"'{end_time}'", 1).strip()
    
    return pd.DataFrame(metrics).set_index("Architecture"), display_raw_q, display_opt_q

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
    start_time_str = c1.text_input("Start Time (HH:MM:SS)", value="14:00:00")
    end_time_str = c2.text_input("End Time (HH:MM:SS)", value="14:15:00")

    try:
        start_time_obj = datetime.strptime(start_time_str, "%H:%M:%S").time()
        end_time_obj = datetime.strptime(end_time_str, "%H:%M:%S").time()
    except ValueError:
        st.error("⚠️ Please enter time in strict HH:MM:SS format")
        start_time_obj = datetime(2024, 4, 11, 14, 0).time()
        end_time_obj = datetime(2024, 4, 11, 14, 15).time()

    start_dt = datetime.combine(st.date_input("Start Date", value=default_date), start_time_obj)
    end_dt = datetime.combine(st.date_input("End Date", value=default_date), end_time_obj)

    res = st.radio("Interval Resolution", ["1-Sec", "1-Min"])
    table = "ohlcv_1s" if res == "1-Sec" else "ohlcv_1m"
    
    st.divider()
    
    auto_refresh = st.toggle("🔴 Live Auto-Refresh", value=False)
    if auto_refresh:
        st_autorefresh(interval=2000, limit=None, key="live_dashboard_refresh")
    
    if st.button("Refresh Data Manually", use_container_width=True):
        st.cache_data.clear()

st.title("📈 Query Stack: Temporal Database Prototype")
tab_market, tab_bench = st.tabs(["📊 Market Analysis", "⚡ Optimization Benchmarks"])

with tab_market:
    df = fetch_ohlcv_data(selected_symbol, table, start_dt, end_dt)
    
    if df.empty:
        st.warning("No data found for this timeframe. Ensure the historical replay or live ingester is running.")
    else:
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
        
        fig.add_trace(go.Candlestick(
            x=df_sorted['ts_bucket'], open=df_sorted['open_price'], high=df_sorted['high_price'], 
            low=df_sorted['low_price'], close=df_sorted['close_price'], name='OHLC'
        ), row=1, col=1)
        
        fig.add_trace(go.Scatter(
            x=df_sorted['ts_bucket'], y=df_sorted['sma_20'], line=dict(color='orange', width=2), name='20-SMA'
        ), row=1, col=1)

        fig.add_trace(go.Bar(
            x=df_sorted['ts_bucket'], y=df_sorted['volume'], name='Volume', marker_color='#26A69A'
        ), row=2, col=1)

        fig.update_layout(
            height=600, 
            xaxis_rangeslider_visible=False, 
            margin=dict(t=0, b=0),
            uirevision='constant'  
        )
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Time Range Query Results")
        st.dataframe(df, use_container_width=True)

with tab_bench:
    st.header("System Performance Benchmarks")
    
    if "bench_df" not in st.session_state:
        st.session_state.bench_df = None
        st.session_state.raw_sql = None
        st.session_state.opt_sql = None

    st.subheader("1. Query Latency Analysis")
    st.markdown("Measures the time required to compute total trading volume across the selected time interval.")
    
    col_btn1, col_btn2, _ = st.columns([2, 1, 3])
    with col_btn1:
        if st.button("Run Live Analytics Benchmark", type="primary", use_container_width=True):
            with st.spinner("Executing queries..."):
                bench_df, raw_sql, opt_sql = run_live_benchmark(selected_symbol, start_dt, end_dt)
                st.session_state.bench_df = bench_df
                st.session_state.raw_sql = raw_sql
                st.session_state.opt_sql = opt_sql
    with col_btn2:
        if st.button("Clear Results", use_container_width=True):
            st.session_state.bench_df = None

    if st.session_state.bench_df is not None:
        bench_df = st.session_state.bench_df
        raw_sql = st.session_state.raw_sql
        opt_sql = st.session_state.opt_sql
        
        raw_arch_name = "LZ4 Arrays (Unnested)" if "LZ4" in bench_df.index[0] else "Standard DB (Raw Ticks)"
        
        raw_l = bench_df.loc[raw_arch_name, "Latency (ms)"]
        opt_l = bench_df.loc["Optimized (Aggregates)", "Latency (ms)"]
        
        if opt_l > 0:
            st.success(f"🔥 **Optimization Result:** Aggregate queries are **{raw_l/opt_l:.1f}x faster** than raw scans.")

        st.markdown("#### Detailed Execution Metrics")
        display_df = bench_df.copy()
        display_df["Avg Time per Row (μs)"] = (display_df["Latency (ms)"] / display_df["Rows Processed"] * 1000).replace([np.inf, -np.inf], 0).round(2).fillna(0)
        
        st.dataframe(
            display_df.style.highlight_min(subset=["Latency (ms)"], color="#09561b"), 
            use_container_width=True
        )

        with st.expander("🔍 View Executed SQL Queries"):
            st.caption(f"{raw_arch_name} Query")
            st.code(raw_sql, language="sql")
            st.caption("Optimized DB Query (Aggregates)")
            st.code(opt_sql, language="sql")

    st.divider()

    st.subheader("2. Storage Optimization Proof")
    stats = fetch_storage_stats()
    
    if not stats.empty:
        raw_table_name = 'raw_ticks_bucketed' if 'raw_ticks_bucketed' in stats['Table'].values else 'raw_ticks'
        
        if raw_table_name in stats['Table'].values and 'ohlcv_1m' in stats['Table'].values:
            raw_b = stats[stats['Table'] == raw_table_name]['Bytes'].values[0]
            opt_b = stats[stats['Table'] == 'ohlcv_1m']['Bytes'].values[0]
            
            if raw_b > 0:
                savings = 100 - ((opt_b / raw_b) * 100)
                st.info(f"💾 **Storage Efficiency:** Aggregating data down to 1-Minute blocks reduces the total physical footprint by **{savings:.2f}%** compared to raw ticks.")

        display_stats = stats[['Table', 'Size', 'Bytes']].sort_values(by="Bytes", ascending=False)
        display_stats['Share of DB (%)'] = ((display_stats['Bytes'] / display_stats['Bytes'].sum()) * 100).round(2)
        
        st.markdown("#### Table Sizes on Disk")
        st.dataframe(display_stats.drop(columns=['Bytes']), use_container_width=True)
