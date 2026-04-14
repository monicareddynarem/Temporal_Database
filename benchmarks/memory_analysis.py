import numpy as np
import matplotlib.pyplot as plt
import time
import sys
import os
import copy

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_srcs.mock_gen import generate_naive_batches
from utils.connection import get_db_connection
from benchmarks.index_vs_noindex import ensure_partition, reset_db

from datetime import datetime


# ---------------- SETUP ---------------- #

def setup_bucket_table(compression=False):
    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:

            cursor.execute("DROP TABLE IF EXISTS raw_ticks_bucketed CASCADE;")

            cursor.execute("""
                CREATE TABLE raw_ticks_bucketed (
                    bucket_ts TIMESTAMP,
                    symbol TEXT,
                    prices REAL[],
                    volumes INT[],
                    offsets_ms INT[]
                ) PARTITION BY RANGE (bucket_ts);
            """)

            if compression:
                cursor.execute("ALTER TABLE raw_ticks_bucketed ALTER COLUMN prices SET COMPRESSION lz4;")
                cursor.execute("ALTER TABLE raw_ticks_bucketed ALTER COLUMN volumes SET COMPRESSION lz4;")
                cursor.execute("ALTER TABLE raw_ticks_bucketed ALTER COLUMN offsets_ms SET COMPRESSION lz4;")

        conn.commit()
    finally:
        conn.close()


# ---------------- SIZE ---------------- #

def get_table_size():
    conn = get_db_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT SUM(pg_total_relation_size(relid))
                FROM pg_catalog.pg_statio_user_tables
                WHERE relname LIKE 'raw_ticks_bucketed%';
            """)
            size = cursor.fetchone()[0] or 0
    finally:
        conn.close()

    return size / 1024  # KB


# ---------------- INGEST ---------------- #

def ingest_and_measure(data_stream):
    conn = get_db_connection()

    sizes = []

    try:
        for i, (batch, v_time, gen_time, total) in enumerate(data_stream):

            loop_start = time.time()

            # ---- BUCKETING ----
            bucket = {}

            for symbol, price, volume, ts in batch:
                bucket_ts = ts.replace(second=(ts.second // 30) * 30, microsecond=0)

                if bucket_ts not in bucket:
                    bucket[bucket_ts] = {
                        "symbol": symbol,
                        "prices": [],
                        "volumes": [],
                        "offsets": []
                    }

                offset = int((ts - bucket_ts).total_seconds() * 1000)

                bucket[bucket_ts]["prices"].append(price)
                bucket[bucket_ts]["volumes"].append(volume)
                bucket[bucket_ts]["offsets"].append(offset)

            # ---- INSERT ----
            with conn.cursor() as cursor:
                for bucket_ts, data in bucket.items():

                    target_table = ensure_partition(
                        cursor,
                        bucket_ts,
                        table="raw_ticks_bucketed"
                    )

                    cursor.execute(
                        f"""
                        INSERT INTO {target_table}
                        (bucket_ts, symbol, prices, volumes, offsets_ms)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            bucket_ts,
                            data["symbol"],
                            data["prices"],
                            data["volumes"],
                            data["offsets"]
                        )
                    )

            conn.commit()

            # ---- SIZE ----
            size = get_table_size()
            sizes.append(size)

            print(
                f"[{i}] Size: {size:.2f} KB | "
                f"Ticks: {total} | "
                f"V-Clock: {v_time.strftime('%H:%M:%S')}"
            )

            # ---- REALISTIC STREAMING ----
            elapsed = time.time() - loop_start
            if elapsed < 1:
                time.sleep(1 - elapsed)

    except KeyboardInterrupt:
        print('Interrupted by user')
    finally:
        conn.close()

    return sizes


# ---------------- MAIN ---------------- #

def main():
    duration = 20
    speed = 5

    data_stream = list(generate_naive_batches(duration, speed))

    # -------- NO COMPRESSION --------
    print("\n--- WITHOUT COMPRESSION ---")
    reset_db()
    setup_bucket_table(compression=False)

    sizes_no_comp = ingest_and_measure(copy.deepcopy(data_stream))


    # -------- WITH COMPRESSION --------
    print("\n--- WITH LZ4 COMPRESSION ---")
    reset_db()
    setup_bucket_table(compression=True)

    sizes_comp = ingest_and_measure(copy.deepcopy(data_stream))

    compression_ratio = np.array(sizes_no_comp) / np.array(sizes_comp)

    # -------- PLOT --------
    plt.figure(figsize=(10, 6))

    plt.plot(sizes_no_comp, label="No Compression")
    plt.plot(sizes_comp, label="LZ4 Compression")

    plt.title("Storage Growth: Compression vs No Compression")
    plt.xlabel("Time (batches)")
    plt.ylabel("Size (KB)")
    plt.legend()

    plt.savefig("./plots/memory_compression_plot.png")
    plt.show()


if __name__ == "__main__":
    main()