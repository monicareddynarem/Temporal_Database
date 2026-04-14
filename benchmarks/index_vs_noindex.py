import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import time
import io
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_srcs.mock_gen import generate_naive_batches
from utils.connection import get_db_connection

from datetime import datetime, timedelta


# ============================================================
# DB SETUP — raw_ticks
# ============================================================

def reset_db():
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS raw_ticks CASCADE;")
    conn.commit()
    conn.close()


# ---------- raw_ticks schemas ----------

def apply_raw_schema_A():
    """raw_ticks: simple non-partitioned table"""
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


def apply_raw_schema_B():
    """raw_ticks: range-partitioned on ts"""
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


def apply_raw_schema_C():
    """raw_ticks: range-partitioned on ts + index"""
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
        cursor.execute("CREATE INDEX idx_raw_ticks_ts ON raw_ticks(ts);")
    conn.commit()
    conn.close()




# ============================================================
# PARTITION HELPERS
# ============================================================

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


# ============================================================
# QUERY TESTS — raw_ticks
# ============================================================

def run_query_tests(n_queries=100):
    conn = get_db_connection()
    latencies = []
    throughputs = []
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MIN(ts), MAX(ts) FROM raw_ticks;")
            data_start, data_end = cursor.fetchone()

        if data_start is None or data_end is None:
            print("No data found in raw_ticks!")
            return [], []

        total_range = (data_end - data_start).total_seconds()
        epsilon = 1e-9

        for _ in range(n_queries):
            window_size = np.random.uniform(10, min(120, total_range))
            window = timedelta(seconds=window_size)
            offset = np.random.uniform(0, max(total_range - window_size, 0))
            t1 = data_start + timedelta(seconds=offset)
            t2 = t1 + window

            q = "SELECT COUNT(*) FROM raw_ticks WHERE ts BETWEEN %s AND %s"
            start = time.time()
            with conn.cursor() as cursor:
                cursor.execute(q, (t1, t2))
                result = cursor.fetchone()[0]
            elapsed = time.time() - start

            latencies.append(elapsed)
            throughputs.append(result / max(elapsed, epsilon))
    finally:
        conn.close()
    return latencies, throughputs




# ============================================================
# CORE INGEST TEST (raw_ticks )
# ============================================================

def run_test(data_stream, use_partition):
    
    latencies = []
    db_latencies = []

    conn = get_db_connection()
    try:
        for batch, v_time, gen_time, total in data_stream:
            loop_start = time.time()

            df = pd.DataFrame(batch, columns=['symbol', 'price', 'volume', 'ts'])

            # ---- raw_ticks write ----
            f = io.StringIO()
            df.to_csv(f, sep='\t', header=False, index=False)
            f.seek(0)

            db_start = time.time()
            with conn.cursor() as cursor:
                target_table = (
                    ensure_partition(cursor, v_time)
                    if use_partition else "raw_ticks"
                )
                cursor.copy_from(
                    f, target_table, sep='\t',
                    columns=('symbol', 'price', 'volume', 'ts')
                )
            conn.commit()
            db_time = time.time() - db_start
            db_latencies.append(db_time)

            elapsed = time.time() - loop_start
            latencies.append(elapsed)

            print(
                f"[IST:{datetime.now().strftime('%H:%M:%S')}] "
                f"Ticks: {total:5d} | "
                f"Gen: {gen_time*1000:6.1f}ms | "
                f"Raw DB: {db_time*1000:6.1f}ms | "
                f"Loop: {elapsed:.3f}s | "
                f"V-Clock: {v_time.strftime('%H:%M:%S')}"
            )

            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            else:
                print(f"  ! LAGGING: loop took {elapsed:.2f}s")

    except KeyboardInterrupt:
        print("\nStopped by user!")
    finally:
        conn.close()

    return latencies, db_latencies


# ============================================================
# PLOTTING
# ============================================================

SCHEMA_COLORS = {
    "Schema A": "#1f77b4",
    "Schema B": "#ff7f0e",
    "Schema C": "#2ca02c",
}
AVG_STYLE = dict(linestyle='--', linewidth=1.2, alpha=0.85)
SMOOTH_W = 5


def moving_avg(x, w=5):
    if len(x) < w:
        return np.array(x)
    return np.convolve(x, np.ones(w) / w, mode='valid')


def _plot_metric(ax, results_dict, key_idx, title, xlabel, ylabel,
                 smooth_w=SMOOTH_W, floor=None):
    """Generic helper to plot one smoothed metric across schemas."""
    for name, result_tuple in results_dict.items():
        color = SCHEMA_COLORS[name]
        data = np.array(result_tuple[key_idx])
        if floor is not None:
            data = data[data > floor]
        if len(data) == 0:
            print(f"WARNING: {name} empty → inserting zeros")
            data = np.array([0])

        smooth = moving_avg(data, smooth_w)
        mean_val = np.mean(data)
        ax.plot(smooth, label=f"{name}", color=color, linewidth=1.4, alpha=0.8)
        ax.axhline(mean_val, color=color,
                   label=f"{name} avg ({mean_val*1000:.2f} ms)",
                   **AVG_STYLE)
    ax.set_title(title, fontweight='bold')
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.4f'))
    ax.legend(ncol=2, fontsize=8)
    ax.grid(True, alpha=0.3)


def plot_raw_results(results):
    """
    Plots raw_ticks results.
    results[schema] = (lat, db_lat, query_lat, tpts)
    """
    os.makedirs('./plots', exist_ok=True)
    fig, axes = plt.subplots(3, 1, figsize=(13, 18))
    fig.suptitle("raw_ticks – Schema Benchmark", fontsize=15,
                 fontweight='bold', y=1.01)

    _plot_metric(axes[0], results, 1, "DB Write Latency", "Batch #", "DB Latency (sec)")
    _plot_metric(axes[1], results, 0, "End-to-End Latency", "Batch #", "Latency (sec)")

    ax = axes[2]
    for name, (lat, db_lat, query_lat, tpts) in results.items():
        color = SCHEMA_COLORS[name]
        q_arr = np.array(query_lat)
        q_arr = np.maximum(q_arr, 1e-6)
        valid = q_arr
        if len(valid) == 0:
            #print(f"  WARNING: {name} has no valid query latency – skipping")
            print(f"WARNING: {name} empty → inserting zeros")
            valid = np.array([0])
            #continue
        smooth = moving_avg(valid, SMOOTH_W)
        mean_val = np.mean(valid)
        ax.plot(smooth, label=f"{name}", color=color, linewidth=1.4, alpha=0.8)
        ax.axhline(mean_val, color=color,
                   label=f"{name} avg ({mean_val*1000:.2f} ms)", **AVG_STYLE)
    ax.set_title("Query Latency ", fontweight='bold')
    ax.set_xlabel("Query #")
    ax.set_ylabel("Latency (sec)")
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter('%.4f'))
    ax.legend(ncol=2, fontsize=8)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig("./plots/raw_ticks_latency.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("Saved → ./plots/raw_ticks_latency.png")

    # Throughput
    fig, ax = plt.subplots(figsize=(11, 6))
    epsilon = 1e-6
    for name, (lat, db_lat, query_lat, tpts) in results.items():
        color = SCHEMA_COLORS[name]
        tp = np.array(tpts)
        tp_pos = tp
        if len(tp_pos) == 0:
            continue
        tp = np.clip(tp, epsilon, np.percentile(tp_pos, 95))
        smooth = moving_avg(tp, SMOOTH_W)
        mean_val = np.mean(smooth)
        ax.plot(smooth, label=f"{name}", color=color, linewidth=1.4,
                marker='o', markersize=2, alpha=0.8)
        ax.axhline(mean_val, color=color,
                   label=f"{name} avg ({mean_val:,.0f} rows/s)", **AVG_STYLE)
    ax.set_title("raw_ticks – Query Throughput (rows/sec)", fontweight='bold')
    ax.set_xlabel("Query #")
    ax.set_ylabel("Throughput (rows/sec)")
    ax.set_yscale('log')
    ax.legend(ncol=2, fontsize=8)
    ax.grid(True, alpha=0.3, which='both')
    plt.tight_layout()
    plt.savefig("./plots/raw_ticks_throughput.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("Saved → ./plots/raw_ticks_throughput.png")





# ============================================================
# MAIN DRIVER
# ============================================================

def main():
    raw_results    = {}

    duration = 20
    speed = 5

    schema_configs = [
        ("Schema A", apply_raw_schema_A, False),
        ("Schema B", apply_raw_schema_B, True),
        ("Schema C", apply_raw_schema_C, True),
    ]

    for name, raw_fn, use_partition in schema_configs:
        print(f"\n{'='*60}")
        print(f"  Running {name}  (partition={use_partition})")
        print(f"{'='*60}")

        reset_db()
        raw_fn()
        

        data_stream = generate_naive_batches(duration, speed)
        lat, db_lat = run_test(
            data_stream, use_partition=use_partition
        )

        # raw_ticks query benchmark
        query_lat, tpts = run_query_tests()
        raw_results[name] = (lat, db_lat, query_lat, tpts)
        
    try:
        plot_raw_results(raw_results)
    except KeyboardInterrupt:
        print("\nPlot window closed manually.")


if __name__ == "__main__":
    main()