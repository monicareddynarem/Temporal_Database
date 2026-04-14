import numpy as np
import matplotlib.pyplot as plt
import time
import io
import pandas as pd
import sys
import os
import copy
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from data_srcs.mock_gen import generate_naive_batches
from utils.connection import get_db_connection

from datetime import datetime, timedelta


#   DB SETUP   #

def reset_db():
    conn = get_db_connection()
    
    # Add this safeguard!
    if conn is None:
        print("CRITICAL: Could not connect to the database. Check your credentials in connection.py.")
        sys.exit(1) # Stop the script safely
        
    try:
        with conn.cursor() as cursor:
            # ... your existing reset logic (drop tables, etc) ...
            pass
        conn.commit()
    finally:
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


#   PARTITION HANDLING   #

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

#   QUERY TEST   #

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

#   CORE TEST   #

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


#   MAIN DRIVER   #
def moving_avg(x, w=5):
    return np.convolve(x, np.ones(w)/w, mode='valid')

def measure_query_latency(conn, query, params=None):
    with conn.cursor() as cursor:
        start = time.time()
        cursor.execute(query, params)
        cursor.fetchall()  # force execution
        return time.time() - start

def main():
    results = {}

    duration = 20
    speed = 5

    # Schema A
    reset_db()
    apply_schema_A()
    data_stream = list(generate_naive_batches(duration, speed))
    stream_A = copy.deepcopy(data_stream)
    stream_B = copy.deepcopy(data_stream)
    stream_C = copy.deepcopy(data_stream)
    lat, db_lat = run_test(stream_A, use_partition=False)
    query_lat,tpts = run_query_tests()
    results["Schema A"] = (lat, db_lat, query_lat, tpts)



    # Schema B
    reset_db()
    apply_schema_B()
    #data_stream = generate_naive_batches(duration, speed)
    lat, db_lat = run_test(stream_B, use_partition=True)
    query_lat,tpts  = run_query_tests()
    results["Schema B"] = (lat, db_lat, query_lat,tpts)


    # Schema C
    reset_db()
    apply_schema_C()
    #data_stream = generate_naive_batches(duration, speed)
    lat, db_lat = run_test(stream_C, use_partition=True)
    query_lat,tpts = run_query_tests()
    results["Schema C"] = (lat, db_lat, query_lat, tpts)


    try:
        fig, axes = plt.subplots(3, 1, figsize=(12, 18))

        # -------- DB LATENCY --------
        for name, (lat, db_lat, query_lat,tpts) in results.items():
            smooth_db = moving_avg(db_lat, w=5)
            axes[0].plot(smooth_db, label=f"{name} (DB)")
            print(f"{name} Avg DB Latency: {np.mean(db_lat):.5f}")

        axes[0].set_title("DB Write Latency Comparison")
        axes[0].set_xlabel("Batch")
        axes[0].set_ylabel("DB Latency (sec)")
        axes[0].legend()


        # -------- TOTAL LATENCY --------
        for name, (lat, db_lat, query_lat,tpts) in results.items():
            smooth_lat = moving_avg(lat, w=5)
            axes[1].plot(smooth_lat, label=f"{name}")
            print(f"{name} Avg Total Latency: {np.mean(lat):.5f}")

        axes[1].set_title("End-to-End Latency Comparison")
        axes[1].set_xlabel("Batch")
        axes[1].set_ylabel("Latency (sec)")
        axes[1].legend()


        # -------- QUERY LATENCY --------
        for name, (lat, db_lat, query_lat,tpts) in results.items():
            smooth_q = moving_avg(query_lat, w=3)
            axes[2].plot(smooth_q, label=f"{name}")
            print(f"{name} Avg Query Latency: {np.mean(query_lat):.5f}")

        axes[2].set_title("Query Latency Comparison")
        axes[2].set_xlabel("Query #")
        axes[2].set_ylabel("Latency (sec)")
        axes[2].legend()


        plt.tight_layout()
        plt.savefig("./plots/combined_latency_plot.png")
        plt.close()


        plt.figure(figsize=(10, 6))

        epsilon = 1e-6

        for name, (lat, db_lat, query_lat, tpts) in results.items():
            smooth_tp = np.array(moving_avg(tpts, w=5))

            smooth_tp = np.clip(
                smooth_tp,
                epsilon,
                np.percentile(smooth_tp, 95)
            )

            plt.plot(smooth_tp, label=f"{name}", marker='o', markersize=3)

            print(f"{name} Avg Query Throughput: {np.mean(smooth_tp):.5f}")

        plt.legend()
        plt.title("Query Throughput Comparison (rows/sec)")
        plt.xlabel("Query #")
        plt.ylabel("Throughput (rows/sec)")
        plt.yscale('log')

        plt.savefig("./plots/throughput_plot.png")
        plt.close()


    except KeyboardInterrupt:
        print("\nPlot window closed manually (Ctrl+C)")


if __name__ == "__main__":
    main()