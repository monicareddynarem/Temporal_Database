import numpy as np
import matplotlib.pyplot as plt
import time
import io
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from data_srcs.mock_gen import generate_naive_batches
from utils.connection import get_db_connection

from datetime import datetime, timedelta


# ---------------- DB SETUP ---------------- #

def reset_db():
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS raw_ticks CASCADE;")
    conn.commit()
    conn.close()


def apply_schema_A():
    """Simple non-partitioned table"""
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE raw_ticks (
                symbol TEXT,
                price FLOAT,
                volume INT,
                ts TIMESTAMP
            );
        """)
    conn.commit()
    conn.close()


def apply_schema_B():
    """Partitioned table (range on timestamp)"""
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE raw_ticks (
                symbol TEXT,
                price FLOAT,
                volume INT,
                ts TIMESTAMP
            ) PARTITION BY RANGE (ts);
        """)
    conn.commit()
    conn.close()


def apply_schema_C():
    """Partitioned + index"""
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE raw_ticks (
                symbol TEXT,
                price FLOAT,
                volume INT,
                ts TIMESTAMP
            ) PARTITION BY RANGE (ts);
        """)

        cursor.execute("""
            CREATE INDEX idx_raw_ticks_ts ON raw_ticks(ts);
        """)
    conn.commit()
    conn.close()


# ---------------- PARTITION HANDLING ---------------- #

def ensure_partition(cursor, ts, table='raw_ticks'):
    date_str = ts.strftime('%Y_%m_%d')
    start_str = ts.strftime('%Y-%m-%d 00:00:00')
    end_str = (ts + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')

    p_name = f"{table}_{date_str}"

    cursor.execute("SELECT 1 FROM pg_class WHERE relname = %s", (p_name,))
    if not cursor.fetchone():
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {p_name}
            PARTITION OF {table}
            FOR VALUES FROM ('{start_str}') TO ('{end_str}')
        """)

    return p_name

# ---------------- QUERY TEST ---------------- #

def run_query_tests(n_queries=100):
    conn = get_db_connection()
    latencies = []
    throughputs = []

    try:
        with conn.cursor() as cursor:
            #  Get actual data bounds
            cursor.execute("SELECT MIN(ts), MAX(ts) FROM raw_ticks;")
            data_start, data_end = cursor.fetchone()

        # Safety check
        if data_start is None or data_end is None:
            print("No data found in table!")
            return [], []

        total_range = (data_end - data_start).total_seconds()
        epsilon = 1e-9

        for i in range(n_queries):
            #  Randomize window size (better than linear growth)
            window_size = np.random.uniform(10, min(120, total_range))
            window = timedelta(seconds=window_size)

            #  Pick random valid end time
            offset = np.random.uniform(0, total_range - window_size)
            t1 = data_start + timedelta(seconds=offset)
            t2 = t1 + window

            q = """
                SELECT COUNT(*) FROM raw_ticks
                WHERE ts BETWEEN %s AND %s
            """

            start = time.time()

            with conn.cursor() as cursor:
                cursor.execute(q, (t1, t2))
                result = cursor.fetchone()[0]

            elapsed = time.time() - start

            latencies.append(elapsed)

            #  Safe throughput calculation
            throughput = result / max(elapsed, epsilon)
            throughputs.append(throughput)

    finally:
        conn.close()

    return latencies, throughputs

# ---------------- CORE TEST ---------------- #

def run_test(data_stream,use_partition):
    latencies = []
    db_latencies = []
    conn = get_db_connection()
    try:
        for batch, v_time, gen_time, total in data_stream:
            start = time.time()

            df = pd.DataFrame(batch, columns=['symbol', 'price', 'volume', 'ts'])

            f = io.StringIO()
            df.to_csv(f, sep='\t', header=False, index=False)
            f.seek(0)

            db_start = time.time()

            with conn.cursor() as cursor:
                if use_partition:
                    target_table = ensure_partition(cursor, v_time)
                else:
                    target_table = "raw_ticks"

                cursor.copy_from(
                    f,
                    target_table,
                    sep='\t',
                    columns=('symbol', 'price', 'volume', 'ts')
                )

            conn.commit()
            db_time = time.time() - db_start

            elapsed = time.time() - start
            latencies.append(elapsed)
            db_latencies.append(db_time)

            print(
                f"[IST:{datetime.now().strftime('%H:%M:%S')}] "
                f"Ticks: {total} | "
                f"Gen: {gen_time*1000:.1f}ms | "
                f"DB Write: {db_time*1000:.1f}ms | "
                f"Loop: {elapsed:.3f}s | "
                f"V-Clock: {v_time.strftime('%H:%M:%S')}"
            )

            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            else:
                print(f"  ! LAGGING: loop took {elapsed:.2f}s")

    except KeyboardInterrupt:
        print("\n Stopped by user!")

    finally:
        conn.close()
    
    return latencies,db_latencies


# ---------------- MAIN DRIVER ---------------- #
def moving_avg(x, w=5):
    return np.convolve(x, np.ones(w)/w, mode='valid')

def measure_query_latency(conn, query, params=None):
    with conn.cursor() as cursor:
        start = time.time()
        cursor.execute(query, params)
        cursor.fetchall()  # force execution
        return time.time() - start
# ---------------- PLOTTING (FIXED) ---------------- #

def plot_results(results):
    import matplotlib.ticker as ticker

    SCHEMA_COLORS = {
        "Schema A": "#1f77b4",
        "Schema B": "#ff7f0e",
        "Schema C": "#2ca02c",
    }
    AVG_STYLE = dict(linestyle='--', linewidth=1.2, alpha=0.85)
    SMOOTH_W_BATCH = 10   # wider window → cleaner trend lines
    SMOOTH_W_QUERY = 5

    def moving_avg(x, w):
        if len(x) < w:
            return np.array(x)
        return np.convolve(x, np.ones(w) / w, mode='valid')

    fig, axes = plt.subplots(3, 1, figsize=(13, 18))
    fig.suptitle("DB Schema Benchmark", fontsize=15, fontweight='bold', y=1.01)

    # ── 1. DB WRITE LATENCY ──────────────────────────────────────────────── #
    ax = axes[0]
    for name, (lat, db_lat, query_lat, tpts) in results.items():
        color = SCHEMA_COLORS[name]
        smooth = moving_avg(db_lat, SMOOTH_W_BATCH)
        mean_val = np.mean(db_lat)

        ax.plot(smooth, label=f"{name}", color=color, linewidth=1.4, alpha=0.8)
        ax.axhline(mean_val, color=color, label=f"{name} avg ({mean_val*1000:.2f} ms)",
                   **AVG_STYLE)

    ax.set_title("DB Write Latency Comparison", fontweight='bold')
    ax.set_xlabel("Batch #")
    ax.set_ylabel("DB Latency (sec)")
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.4f'))
    ax.legend(ncol=2, fontsize=8)
    ax.grid(True, alpha=0.3)

    # ── 2. END-TO-END LATENCY ────────────────────────────────────────────── #
    ax = axes[1]
    for name, (lat, db_lat, query_lat, tpts) in results.items():
        color = SCHEMA_COLORS[name]
        smooth = moving_avg(lat, SMOOTH_W_BATCH)
        mean_val = np.mean(lat)

        ax.plot(smooth, label=f"{name}", color=color, linewidth=1.4, alpha=0.8)
        ax.axhline(mean_val, color=color, label=f"{name} avg ({mean_val*1000:.2f} ms)",
                   **AVG_STYLE)

    ax.set_title("End-to-End Latency Comparison", fontweight='bold')
    ax.set_xlabel("Batch #")
    ax.set_ylabel("Latency (sec)")
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.4f'))
    ax.legend(ncol=2, fontsize=8)
    ax.grid(True, alpha=0.3)

    # ── 3. QUERY LATENCY ────────────────────────────────────────────────── #
    ax = axes[2]
    for name, (lat, db_lat, query_lat, tpts) in results.items():
        color = SCHEMA_COLORS[name]

        # Filter out near-zero artefacts (empty partition hits returning ~0 s)
        q_arr = np.array(query_lat)
        valid  = q_arr[q_arr > 1e-4]          # drop sub-0.1 ms ghost results
        if len(valid) == 0:
            print(f"  WARNING: {name} has no valid query latency data — skipping")
            continue

        smooth = moving_avg(valid, SMOOTH_W_QUERY)
        mean_val = np.mean(valid)

        ax.plot(smooth, label=f"{name}", color=color, linewidth=1.4, alpha=0.8)
        ax.axhline(mean_val, color=color, label=f"{name} avg ({mean_val*1000:.2f} ms)",
                   **AVG_STYLE)

    ax.set_title("Query Latency Comparison  (sub-0.1 ms ghost hits excluded)", fontweight='bold')
    ax.set_xlabel("Query #")
    ax.set_ylabel("Latency (sec)")
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.4f'))
    ax.legend(ncol=2, fontsize=8)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("./plots/combined_latency_plot.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("Saved → ./plots/combined_latency_plot.png")

    # ── 4. THROUGHPUT ───────────────────────────────────────────────────── #
    fig, ax = plt.subplots(figsize=(11, 6))
    epsilon = 1e-6

    for name, (lat, db_lat, query_lat, tpts) in results.items():
        color = SCHEMA_COLORS[name]
        tp = np.array(tpts)
        tp = np.clip(tp, epsilon, np.percentile(tp[tp > epsilon], 95))
        smooth = moving_avg(tp, SMOOTH_W_QUERY)
        mean_val = np.mean(smooth)

        ax.plot(smooth, label=f"{name}", color=color, linewidth=1.4,
                marker='o', markersize=2, alpha=0.8)
        ax.axhline(mean_val, color=color, label=f"{name} avg ({mean_val:,.0f} rows/s)",
                   **AVG_STYLE)

    ax.set_title("Query Throughput Comparison (rows/sec)", fontweight='bold')
    ax.set_xlabel("Query #")
    ax.set_ylabel("Throughput (rows/sec)")
    ax.set_yscale('log')
    ax.legend(ncol=2, fontsize=8)
    ax.grid(True, alpha=0.3, which='both')
    plt.tight_layout()
    plt.savefig("./plots/throughput_plot.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("Saved → ./plots/throughput_plot.png")
    
def main():
    results = {}

    duration = 20
    speed = 5

    # Schema A
    reset_db()
    apply_schema_A()
    data_stream = generate_naive_batches(duration, speed)
    lat, db_lat = run_test(data_stream, use_partition=False)
    query_lat,tpts = run_query_tests()
    results["Schema A"] = (lat, db_lat, query_lat, tpts)



    # Schema B
    reset_db()
    apply_schema_B()
    data_stream = generate_naive_batches(duration, speed)
    lat, db_lat = run_test(data_stream, use_partition=True)
    query_lat,tpts  = run_query_tests()
    results["Schema B"] = (lat, db_lat, query_lat,tpts)


    # Schema C
    reset_db()
    apply_schema_C()
    data_stream = generate_naive_batches(duration, speed)
    lat, db_lat = run_test(data_stream, use_partition=True)
    query_lat,tpts = run_query_tests()
    results["Schema C"] = (lat, db_lat, query_lat, tpts)


    try:
        plot_results(results)

    except KeyboardInterrupt:
        print("\nPlot window closed manually (Ctrl+C)")


if __name__ == "__main__":
    main()