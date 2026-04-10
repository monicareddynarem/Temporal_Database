import streamlit as st
import pandas as pd
import psycopg2
import time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from connection import get_db_connection

# ==========================================
# 1. PAGE CONFIGURATION
# ==========================================
st.set_page_config(
    page_title="Query Stack Dashboard",
    page_icon="📈",
    layout="wide"
)

# ==========================================
# 2. DATABASE FETCH FUNCTIONS
# ==========================================
@st.cache_data(ttl=2)
def fetch_ohlcv_data(symbol, table_name, start_time, end_time):
    """Fetches real aggregated data from your PostgreSQL tables."""
    conn = None
    try:
        conn = get_db_connection()
        query = f"""
            SELECT bucket_time, open_price, high_price, low_price, close_price, total_volume
            FROM {table_name}
            WHERE symbol = %s AND bucket_time >= %s AND bucket_time <= %s
            ORDER BY bucket_time ASC;
        """
        df = pd.read_sql_query(query, conn, params=(symbol, start_time, end_time))
        for col in ['open_price', 'high_price', 'low_price', 'close_price']:
            df[col] = df[col].astype(float)
        return df
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

def fetch_storage_stats():
    """Fetches the actual on-disk size of the tables to prove storage optimization."""
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                'Raw Ticks (Unoptimized)' as table_type,
                pg_size_pretty(pg_total_relation_size('raw_ticks')) as pretty_size,
                pg_total_relation_size('raw_ticks') as bytes
            UNION ALL
            SELECT 
                '1m Aggregates (Optimized)',
                pg_size_pretty(pg_total_relation_size('ohlcv_1m')),
                pg_total_relation_size('ohlcv_1m');
        """
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()

def run_live_benchmark(symbol, start_time, end_time):
    """Compares querying raw ticks vs aggregates, including rows scanned."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    metrics = []
    
    # Test 1: Querying Raw Ticks
    raw_query = "SELECT SUM(volume), COUNT(*) FROM raw_ticks WHERE symbol = %s AND ts >= %s AND ts <= %s;"
    start = time.time()
    cursor.execute(raw_query, (symbol, start_time, end_time))
    raw_result = cursor.fetchone()
    raw_time = (time.time() - start) * 1000
    raw_rows = raw_result[1] if raw_result[1] else 0
    
    metrics.append({
        'Query Architecture': 'Standard DB (Raw Ticks)',
        'Execution Latency (ms)': round(raw_time, 2),
        'Rows Scanned': raw_rows,
        'Rows/Sec Throughput': int((raw_rows / (raw_time / 1000))) if raw_time > 0 else 0
    })

    # Test 2: Querying Aggregates (Optimized)
    agg_query = "SELECT SUM(total_volume), COUNT(*) FROM ohlcv_1m WHERE symbol = %s AND bucket_time >= %s AND bucket_time <= %s;"
    start = time.time()
    cursor.execute(agg_query, (symbol, start_time, end_time))
    agg_result = cursor.fetchone()
    agg_time = (time.time() - start) * 1000
    agg_rows = agg_result[1] if agg_result[1] else 0
    
    metrics.append({
        'Query Architecture': 'Optimized (1m Aggregates)',
        'Execution Latency (ms)': round(agg_time, 2),
        'Rows Scanned': agg_rows,
        'Rows/Sec Throughput': int((agg_rows / (agg_time / 1000))) if agg_time > 0 else 0
    })
    
    cursor.close()
    conn.close()
    return pd.DataFrame(metrics).set_index('Query Architecture')

# ==========================================
# 3. SIDEBAR CONTROLS 
# ==========================================
st.sidebar.title("⚙️ Query Controls")

SYMBOLS = ['GOOGL', 'META', 'TSLA', 'NVDA', 'AMZN', 'NFLX', 'MSFT', 'AAPL', 'TSMC', 'INTC']
selected_symbol = st.sidebar.selectbox("Select Stock Symbol", SYMBOLS)

st.sidebar.subheader("Time Range")
default_start = datetime.now() - timedelta(hours=6)
default_end = datetime.now() + timedelta(hours=6)

start_date = st.sidebar.date_input("Start Date", default_start.date())
start_time = st.sidebar.time_input("Start Time", default_start.time())
end_date = st.sidebar.date_input("End Date", default_end.date())
end_time = st.sidebar.time_input("End Time", default_end.time())

full_start_time = datetime.combine(start_date, start_time)
full_end_time = datetime.combine(end_date, end_time)

resolution_options = {"1-Second OHLCV": "ohlcv_1s", "1-Minute OHLCV": "ohlcv_1m"}
resolution_label = st.sidebar.radio("Data Resolution", list(resolution_options.keys()), index=1)
selected_table = resolution_options[resolution_label]

analysis_mode = st.sidebar.selectbox("Analysis Mode", [
    "Standard Candlestick", 
    "Trend: Moving Average (20-SMA)", 
    "Volatility: Bollinger Bands",
    "Volume: VWAP"
])

st.sidebar.markdown("---")
if st.sidebar.button("🔄 Force Data Refresh", use_container_width=True):
    st.cache_data.clear()

# ==========================================
# 4. MAIN LAYOUT & TABS
# ==========================================
st.title("📈 Query Stack: Temporal Database Prototype")

tab_market, tab_performance = st.tabs(["Market Data Visualization", "DBMS Performance Dashboard"])

# ------------------------------------------
# TAB 1: Market Data Visualization
# ------------------------------------------
with tab_market:
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader(f"Live Market Feed: {selected_symbol}")
    
    df = fetch_ohlcv_data(selected_symbol, selected_table, full_start_time, full_end_time)
    
    if df.empty:
        st.warning(f"No data found for {selected_symbol} in this time range. Adjust your filters or check the generator.")
    else:
        # Advanced Metric Calculations
        current_price = df['close_price'].iloc[-1]
        price_change = current_price - df['close_price'].iloc[0]
        pct_change = (price_change / df['close_price'].iloc[0]) * 100
        volatility = df['close_price'].pct_change().std() * 100 # Rough volatility calc
        
        # Striking Top Metrics
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Latest Price", f"${current_price:.2f}", f"{price_change:.2f} ({pct_change:.2f}%)")
        m2.metric("Total Volume", f"{df['total_volume'].sum():,}")
        m3.metric("Current Volatility", f"{volatility:.3f}%" if pd.notna(volatility) else "N/A")
        m4.metric("Data Points Rendered", f"{len(df):,}")

        st.markdown("---")

        # Create Plotly Subplots
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                            vertical_spacing=0.03, subplot_titles=(f"{resolution_label} Price Action", "Traded Volume"),
                            row_width=[0.2, 0.7])

        # Base Candlestick
        fig.add_trace(go.Candlestick(
            x=df['bucket_time'], open=df['open_price'], high=df['high_price'],
            low=df['low_price'], close=df['close_price'], name='OHLC'
        ), row=1, col=1)

        # Advanced Overlay Logic based on Sidebar Selection
        if analysis_mode == "Trend: Moving Average (20-SMA)" and len(df) >= 20:
            df['SMA_20'] = df['close_price'].rolling(window=20).mean()
            fig.add_trace(go.Scatter(x=df['bucket_time'], y=df['SMA_20'], line=dict(color='#FFA500', width=2), name='20-SMA'), row=1, col=1)

        elif analysis_mode == "Volatility: Bollinger Bands" and len(df) >= 20:
            df['SMA_20'] = df['close_price'].rolling(window=20).mean()
            df['StdDev'] = df['close_price'].rolling(window=20).std()
            df['Upper'] = df['SMA_20'] + (df['StdDev'] * 2)
            df['Lower'] = df['SMA_20'] - (df['StdDev'] * 2)
            
            fig.add_trace(go.Scatter(x=df['bucket_time'], y=df['Upper'], line=dict(color='rgba(173, 216, 230, 0.5)', width=1, dash='dot'), name='Upper Band'), row=1, col=1)
            fig.add_trace(go.Scatter(x=df['bucket_time'], y=df['Lower'], line=dict(color='rgba(173, 216, 230, 0.5)', width=1, dash='dot'), fill='tonexty', fillcolor='rgba(173, 216, 230, 0.1)', name='Lower Band'), row=1, col=1)
            fig.add_trace(go.Scatter(x=df['bucket_time'], y=df['SMA_20'], line=dict(color='#FFA500', width=1.5), name='SMA'), row=1, col=1)

        elif analysis_mode == "Volume: VWAP":
            # Typical Price = (High + Low + Close) / 3
            df['Typical_Price'] = (df['high_price'] + df['low_price'] + df['close_price']) / 3
            df['VWAP'] = (df['Typical_Price'] * df['total_volume']).cumsum() / df['total_volume'].cumsum()
            fig.add_trace(go.Scatter(x=df['bucket_time'], y=df['VWAP'], line=dict(color='#FF1493', width=2), name='VWAP'), row=1, col=1)

        # Volume Bar Chart
        colors = ['#26A69A' if row['close_price'] >= row['open_price'] else '#EF5350' for index, row in df.iterrows()]
        fig.add_trace(go.Bar(
            x=df['bucket_time'], y=df['total_volume'], marker_color=colors, name='Volume'
        ), row=2, col=1)

        fig.update_layout(
            height=650, xaxis_rangeslider_visible=False, showlegend=True, 
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------
# TAB 2: DBMS Performance Dashboard
# ------------------------------------------
with tab_performance:
    st.subheader("⚡ Database Engine Benchmarks")
    st.markdown("Mathematical proof of continuous aggregation efficiency and storage optimization.")
    
    col_bench, col_storage = st.columns([2, 1])
    
    with col_bench:
        st.markdown("#### Execution Latency & I/O Scans")
        if st.button("🚀 Execute Live Benchmark Query", type="primary"):
            with st.spinner("Executing full-table scans on PostgreSQL..."):
                metrics_df = run_live_benchmark(selected_symbol, full_start_time, full_end_time)
                
                # Highlight the winner
                raw_time = metrics_df.loc['Standard DB (Raw Ticks)', 'Execution Latency (ms)']
                agg_time = metrics_df.loc['Optimized (1m Aggregates)', 'Execution Latency (ms)']
                
                if agg_time > 0 and raw_time > 0:
                    speedup = raw_time / agg_time
                    st.success(f"**Performance Gain:** The Continuous Aggregate pipeline is **{speedup:.1f}x faster** than raw full-table scans!")
                
                # Plotly grouped bar chart for striking visuals
                fig_bar = go.Figure(data=[
                    go.Bar(name='Execution Time (ms)', x=metrics_df.index, y=metrics_df['Execution Latency (ms)'], marker_color=['#EF5350', '#26A69A']),
                ])
                fig_bar.update_layout(title="Query Latency Comparison", template="plotly_white", height=350)
                st.plotly_chart(fig_bar, use_container_width=True)
                
                st.dataframe(metrics_df, use_container_width=True)

    with col_storage:
        st.markdown("#### Disk Storage Optimization")
        st.info("Aggregating ticks into 1-minute blocks massively reduces disk space footprint.")
        
        # Live Storage Query
        storage_df = fetch_storage_stats()
        st.dataframe(storage_df[['table_type', 'pretty_size']].set_index('table_type'), use_container_width=True)
        
        # Pie chart of storage usage
        if not storage_df.empty:
            fig_pie = go.Figure(data=[go.Pie(labels=storage_df['table_type'], values=storage_df['bytes'], hole=.5, marker_colors=['#EF5350', '#26A69A'])])
            fig_pie.update_layout(title="Database Size Footprint", showlegend=False, height=300)
            st.plotly_chart(fig_pie, use_container_width=True)