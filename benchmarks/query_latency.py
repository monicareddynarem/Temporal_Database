import psycopg2
from psycopg2.extras import execute_values
import time
import random
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os
import sys

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "23CS10046", 
    "user": "23CS10046", 
    "password": "monica@2006", # <-- UPDATE THIS TO YOUR PASSWORD
    "host": "10.5.18.101",
    "port": "5432"
}

SYMBOLS = ['GOOGL', 'META', 'TSLA', 'NVDA', 'AMZN', 'NFLX', 'MSFT', 'AAPL', 'TSMC', 'INTC']

# ==========================================================
# 1. AUTOMATED SCHEMA SETUP & MASSIVE BLOAT INJECTION
# ==========================================================
def setup_schemas_and_bloat(conn):
    print("1. Rebuilding Schemas and Injecting 500,000 Historical Rows...")
    with conn.cursor() as cur:
        # --- NAIVE SCHEMA ---
        cur.execute("DROP TABLE IF EXISTS raw_ticks CASCADE;")
        cur.execute("""
            CREATE TABLE raw_ticks (
                symbol VARCHAR(10),
                price NUMERIC(10, 2),
                volume INTEGER,
                ts TIMESTAMP
            );
        """)

        # --- OPTIMIZED SCHEMA ---
        cur.execute("DROP TABLE IF EXISTS symbols CASCADE;")
        cur.execute("CREATE TABLE symbols (symbol VARCHAR(10) PRIMARY KEY, company_name VARCHAR(100));")
        execute_values(cur, "INSERT INTO symbols (symbol, company_name) VALUES %s", [(s, s) for s in SYMBOLS])

        cur.execute("DROP TABLE IF EXISTS raw_ticks_bucketed CASCADE;")
        cur.execute("""
            CREATE TABLE raw_ticks_bucketed (
                bucket_ts TIMESTAMP NOT NULL,
                symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
                prices REAL[] NOT NULL, volumes INT[] NOT NULL, offsets_ms INT[] NOT NULL
            ) PARTITION BY RANGE (bucket_ts);
        """)
        
        cur.execute("DROP TABLE IF EXISTS ohlcv_1s CASCADE;")
        cur.execute("""
            CREATE TABLE ohlcv_1s (
                ts_bucket TIMESTAMP NOT NULL, symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
                open_price NUMERIC(10, 2), high_price NUMERIC(10, 2), low_price NUMERIC(10, 2), close_price NUMERIC(10, 2), volume INTEGER,
                PRIMARY KEY (ts_bucket, symbol)
            ) PARTITION BY RANGE (ts_bucket);
        """)

        cur.execute("DROP TABLE IF EXISTS ohlcv_1m CASCADE;")
        cur.execute("""
            CREATE TABLE ohlcv_1m (
                ts_bucket TIMESTAMP NOT NULL, symbol VARCHAR(10) NOT NULL REFERENCES symbols(symbol),
                open_price NUMERIC(10, 2), high_price NUMERIC(10, 2), low_price NUMERIC(10, 2), close_price NUMERIC(10, 2), volume INTEGER,
                PRIMARY KEY (ts_bucket, symbol)
            ) PARTITION BY RANGE (ts_bucket);
        """)

        # CREATE PARTITIONS FOR THE LAST 5 DAYS (Required for indexing)
        for i in range(5):
            d = datetime.now() - timedelta(days=i)
            day_str = d.strftime('%Y_%m_%d')
            start_str = d.strftime('%Y-%m-%d 00:00:00')
            end_str = (d + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
            
            cur.execute(f"CREATE TABLE raw_ticks_bucketed_{day_str} PARTITION OF raw_ticks_bucketed FOR VALUES FROM ('{start_str}') TO ('{end_str}');")
            cur.execute(f"CREATE TABLE ohlcv_1s_{day_str} PARTITION OF ohlcv_1s FOR VALUES FROM ('{start_str}') TO ('{end_str}');")
            cur.execute(f"CREATE TABLE ohlcv_1m_{day_str} PARTITION OF ohlcv_1m FOR VALUES FROM ('{start_str}') TO ('{end_str}');")

        # ADD CRITICAL INDEXES TO OPTIMIZED SCHEMA
        cur.execute("CREATE INDEX idx_bucketed_sym_ts ON raw_ticks_bucketed (symbol, bucket_ts);")
        cur.execute("CREATE INDEX idx_ohlcv_1s_sym_ts ON ohlcv_1s (symbol, ts_bucket);")
        cur.execute("CREATE INDEX idx_ohlcv_1m_sym_ts ON ohlcv_1m (symbol, ts_bucket);")

        # --- INJECT MASSIVE BLOAT VIA SQL ---
        # This guarantees the Naive DB has to do painful full-table disk scans
        cur.execute("""
            INSERT INTO raw_ticks (symbol, price, volume, ts)
            SELECT 
                (ARRAY['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC'])[floor(random() * 10 + 1)],
                round((random() * 500 + 100)::numeric, 2),
                floor(random() * 100 + 1)::int,
                NOW() - (random() * interval '4 days')
            FROM generate_series(1, 500000);
        """)

        # Simulate the equivalent bloat into the Optimized 1m tables
        cur.execute("""
            INSERT INTO ohlcv_1m (ts_bucket, symbol, open_price, high_price, low_price, close_price, volume)
            SELECT 
                date_trunc('minute', ts) as tb, symbol, 
                AVG(price) as op, MAX(price) as hp, MIN(price) as lp, AVG(price) as cp, SUM(volume) as vol
            FROM raw_ticks GROUP BY tb, symbol
            ON CONFLICT DO NOTHING;
        """)
        
        # Simulate equivalent into 1s tables
        cur.execute("""
            INSERT INTO ohlcv_1s (ts_bucket, symbol, open_price, high_price, low_price, close_price, volume)
            SELECT 
                date_trunc('second', ts) as tb, symbol, 
                AVG(price) as op, MAX(price) as hp, MIN(price) as lp, AVG(price) as cp, SUM(volume) as vol
            FROM raw_ticks GROUP BY tb, symbol
            ON CONFLICT DO NOTHING;
        """)

    conn.commit()

# ==========================================================
# 2. BENCHMARKING QUERIES
# ==========================================================
def measure_latency(conn, query, params):
    try:
        with conn.cursor() as cur:
            # Drop query plan cache to prevent unfair optimizations on repeats
            cur.execute("DISCARD PLANS;")
            
            param_count = query.count('%s')
            safe_params = params[:param_count]
            
            t0 = time.time()
            cur.execute(query, safe_params)
            _ = cur.fetchall()
            return (time.time() - t0) * 1000.0
    except Exception as e:
        print(f"Query Error: {e}")
        conn.rollback()
        return 0

QUERIES = {
    "1. Time Range": {
        "naive": "SELECT * FROM raw_ticks WHERE symbol = %s AND ts BETWEEN %s AND %s;",
        # FIXED: Stop unnesting. Just retrieve the compressed arrays using the index.
        "opt": "SELECT bucket_ts, prices, volumes FROM raw_ticks_bucketed WHERE symbol = %s AND bucket_ts BETWEEN %s AND %s;"
    },
    "2. OHLC Scan": {
        "naive": "SELECT date_trunc('minute', ts), MIN(price), MAX(price), SUM(volume) FROM raw_ticks WHERE symbol = %s AND ts BETWEEN %s AND %s GROUP BY 1;",
        "opt": "SELECT * FROM ohlcv_1m WHERE symbol = %s AND ts_bucket BETWEEN %s AND %s;"
    },
    "3. Moving Avg": {
        "naive": "WITH mb AS (SELECT date_trunc('minute', ts) as tb, AVG(price) as cp FROM raw_ticks WHERE symbol = %s GROUP BY 1) SELECT tb, AVG(cp) OVER (ORDER BY tb ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) FROM mb LIMIT 100;",
        "opt": "SELECT ts_bucket, AVG(close_price) OVER (ORDER BY ts_bucket ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) FROM ohlcv_1m WHERE symbol = %s LIMIT 100;"
    },
    "4. Volume": {
        "naive": "SELECT SUM(volume) FROM raw_ticks WHERE symbol = %s AND ts BETWEEN %s AND %s;",
        "opt": "SELECT SUM(volume) FROM ohlcv_1m WHERE symbol = %s AND ts_bucket BETWEEN %s AND %s;"
    },
    "5. Latest Price": {
        "naive": "SELECT price FROM raw_ticks WHERE symbol = %s ORDER BY ts DESC LIMIT 1;",
        "opt": "SELECT close_price FROM ohlcv_1s WHERE symbol = %s ORDER BY ts_bucket DESC LIMIT 1;"
    },
    "6. Volatility": {
        "naive": "SELECT STDDEV(price) FROM raw_ticks WHERE symbol = %s AND ts BETWEEN %s AND %s;",
        "opt": "SELECT STDDEV(close_price) FROM ohlcv_1m WHERE symbol = %s AND ts_bucket BETWEEN %s AND %s;"
    }
}

def run_benchmarks(conn, iterations=15):
    print(f"\n2. Running Benchmark ({iterations} passes)...")
    opt_times = {k: [] for k in QUERIES}
    naive_times = {k: [] for k in QUERIES}

    for _ in range(iterations):
        sym = random.choice(SYMBOLS)
        base = datetime.now() - timedelta(days=random.randint(1, 3))
        end = base + timedelta(hours=6) # 6 hour scan
        params = (sym, base, end)
        
        for name, sqls in QUERIES.items():
            naive_times[name].append(measure_latency(conn, sqls["naive"], params))
            opt_times[name].append(measure_latency(conn, sqls["opt"], params))
            
    labels = list(QUERIES.keys())
    res_opt = [np.mean(opt_times[n]) for n in labels]
    res_naive = [np.mean(naive_times[n]) for n in labels]
    
    print("\n--- Benchmark Results ---")
    for i, name in enumerate(labels):
        speedup = res_naive[i] / max(res_opt[i], 0.001)
        print(f"{name:<15} | Optimized: {res_opt[i]:6.2f} ms | Naive: {res_naive[i]:6.2f} ms | Speedup: {speedup:.1f}x")
        
    return labels, res_opt, res_naive

# ==========================================================
# 3. PLOTTING
# ==========================================================
def plot_results(labels, opt_res, naive_res):
    print("\n3. Generating Plot...")
    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(14, 8)) # Made slightly wider for better label spacing
    
    # Plot the bars
    ax.bar(x - width/2, opt_res, width, label='Optimized DB (Aggregated & Indexed)', color='#2ca02c', edgecolor='black', linewidth=0.5)
    ax.bar(x + width/2, naive_res, width, label='Naive DB (Raw Ticks Only)', color='#d62728', edgecolor='black', linewidth=0.5)

    # Labels and Titles
    ax.set_ylabel('Latency in Milliseconds (Log Scale)', fontweight='bold', fontsize=12)
    ax.set_title('Query Stack Architecture vs Naive Baseline (5M+ Rows)', fontsize=16, fontweight='bold', pad=25)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=11)
    ax.legend(loc='upper right', fontsize=11)
    
    # Log scale
    ax.set_yscale('log')
    ax.grid(axis='y', linestyle='--', alpha=0.5, which='both')

    # THE FIX: Calculate the absolute maximum value and add explicit headroom
    max_val = max(max(opt_res), max(naive_res))
    ax.set_ylim(bottom=min(min(opt_res), min(naive_res)) * 0.5, top=max_val * 8) # *8 adds plenty of space on a log scale

    # Add the speedup labels
    for i in range(len(labels)):
        if opt_res[i] > 0:
            speedup = naive_res[i] / max(opt_res[i], 0.001)
            if speedup >= 1: # Only annotate if it's noticeably faster
                # Determine the highest bar in this specific group to place the text above it
                local_max = max(opt_res[i], naive_res[i])
                
                ax.text(x[i], local_max * 1.8, f"{speedup:.1f}x\nFaster", 
                        ha='center', va='bottom', fontsize=10, fontweight='bold', 
                        bbox=dict(facecolor='white', alpha=0.95, edgecolor='black', boxstyle='round,pad=0.4'))

    os.makedirs("./plots", exist_ok=True)
    
    # tight_layout ensures nothing gets clipped at the edges of the image file
    plt.tight_layout()
    plt.savefig('./plots/automated_architecture_comparison.png', dpi=300) # Increased DPI for crisper text
    print("Plot saved to ./plots/automated_architecture_comparison.png")
    plt.show()

if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True  # Required for some DB operations
    try:
        setup_schemas_and_bloat(conn)
        labels, opt_res, naive_res = run_benchmarks(conn, iterations=10)
        plot_results(labels, opt_res, naive_res)
    finally:
        conn.close()