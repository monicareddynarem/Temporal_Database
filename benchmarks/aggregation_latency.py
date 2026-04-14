import numpy as np
import matplotlib.pyplot as plt
import time
import sys
import os
import copy

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_srcs.mock_gen import generate_naive_batches
from utils.connection import get_db_connection
from benchmarks.index_vs_noindex import reset_db,apply_schema_A,apply_schema_B,apply_schema_C,ensure_partition
from datetime import datetime, timedelta



def ingest_data(data_stream , use_partition=False):
    
    conn = get_db_connection()

    try:
        for batch, v_time, gen_time, total in data_stream:
            start = time.time()

            with conn.cursor() as cursor:

                if use_partition:
                    target_table = ensure_partition(cursor, v_time)
                else:
                    target_table = "raw_ticks"

                for row in batch:
                    cursor.execute(
                        f"INSERT INTO {target_table} VALUES (%s, %s, %s, %s)",
                        row
                    )
                print(
                    f"[IST:{datetime.now().strftime('%H:%M:%S')}] "
                    f"Ticks: {total} | "
                    f"Gen: {gen_time*1000:.1f}ms | "
                    f"V-Clock: {v_time.strftime('%H:%M:%S')}"
                )

            conn.commit()
            elapsed = time.time() - start

            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
    except KeyboardInterrupt:
        print('Interrupted by user')
    finally:
        conn.close()


def run_aggregation_tests(n_queries=50):
    conn = get_db_connection()
    latencies = []

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT MIN(ts), MAX(ts) FROM raw_ticks;")
            data_start, data_end = cursor.fetchone()

        if data_start is None or data_end is None:
            return []

        total_range = (data_end - data_start).total_seconds()

        for _ in range(n_queries):
            window = timedelta(seconds=np.random.uniform(10, 60))
            offset = np.random.uniform(0, total_range - window.total_seconds())

            t1 = data_start + timedelta(seconds=offset)
            t2 = t1 + window

            q = """
                SELECT symbol, AVG(price)
                FROM raw_ticks
                WHERE ts BETWEEN %s AND %s
                GROUP BY symbol
            """

            start = time.time()

            with conn.cursor() as cursor:
                cursor.execute(q, (t1, t2))
                cursor.fetchall()

            latencies.append(time.time() - start)

    finally:
        conn.close()

    return latencies


def moving_avg(x, w=5):
    if len(x) < w:
        return x
    return np.convolve(x, np.ones(w)/w, mode='valid')


def main():
    results = {}

    duration = 20
    speed = 5
    data_stream = list(generate_naive_batches(duration, speed))
    stream_A = copy.deepcopy(data_stream)
    stream_B = copy.deepcopy(data_stream)
    stream_C = copy.deepcopy(data_stream)


    print('Schema A')
    reset_db()
    apply_schema_A()
    ingest_data(stream_A,use_partition=False)
    results["Schema A"] = run_aggregation_tests()

    
    print('Schema B')
    reset_db()
    apply_schema_B()
    ingest_data(stream_B, use_partition=True)
    results["Schema B"] = run_aggregation_tests()

    
    print('Schema C')
    reset_db()
    apply_schema_C()
    ingest_data(stream_C, use_partition=True)
    results["Schema C"] = run_aggregation_tests()

    

    plt.figure(figsize=(10, 6))

    for name, lat in results.items():
        smooth = moving_avg(lat, w=5)
        plt.plot(smooth, label=name)
        print(f"{name} Avg Aggregation Latency: {np.mean(lat):.5f}")

    plt.legend()
    plt.title("Aggregation Latency Comparison")
    plt.xlabel("Query #")
    plt.ylabel("Latency (sec)")

    plt.savefig("./plots/aggregation_latency_plot.png")
    


if __name__ == "__main__":
    main()