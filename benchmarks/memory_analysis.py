import numpy as np
import matplotlib.pyplot as plt
import time
import sys
import os
import random
from datetime import datetime, timedelta
import copy

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.connection import get_db_connection
from benchmarks.index_vs_noindex import ensure_partition, reset_db

# ---------------- SMART DATA GENERATOR ---------------- #

def generate_smart_batches(duration_minutes, n_speed):
    """Generates Random Walk data so LZ4 can actually compress it."""
    symbols = ['GOOGL', 'META', 'TSLA', 'NVDA', 'AMZN', 'NFLX', 'MSFT', 'AAPL', 'TSMC', 'INTC']
    
    # Initialize starting prices for each symbol
    current_prices = {sym: random.uniform(100.0, 500.0) for sym in symbols}
    
    start_v_time = datetime.now()
    end_v_time = start_v_time + timedelta(minutes=duration_minutes)
    virtual_time = start_v_time
    
    ticks_per_v_sec = 100
    ms_per_tick = 1000 / ticks_per_v_sec
    total_generated = 0

    while virtual_time < end_v_time:
        batch = []
        gen_start = time.time()
        
        total_ticks = n_speed * ticks_per_v_sec
        for _ in range(total_ticks):
            sym = random.choice(symbols)
            # Random walk: Price changes by a tiny amount (-$0.25 to +$0.25)
            current_prices[sym] += random.uniform(-0.25, 0.25)
            price = round(current_prices[sym], 2)
            volume = random.randint(1, 100)
            
            batch.append((sym, price, volume, virtual_time))
            virtual_time += timedelta(milliseconds=ms_per_tick)
            
        total_generated += total_ticks
        gen_time = time.time() - gen_start
        
        yield batch, virtual_time, gen_time, total_generated


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
                # Force LZ4 Compression
                cursor.execute("ALTER TABLE raw_ticks_bucketed ALTER COLUMN prices SET COMPRESSION lz4;")
                cursor.execute("ALTER TABLE raw_ticks_bucketed ALTER COLUMN volumes SET COMPRESSION lz4;")
                cursor.execute("ALTER TABLE raw_ticks_bucketed ALTER COLUMN offsets_ms SET COMPRESSION lz4;")
            else:
                # THE FIX: Force Postgres to NOT compress the arrays in the background
                cursor.execute("ALTER TABLE raw_ticks_bucketed ALTER COLUMN prices SET STORAGE EXTERNAL;")
                cursor.execute("ALTER TABLE raw_ticks_bucketed ALTER COLUMN volumes SET STORAGE EXTERNAL;")
                cursor.execute("ALTER TABLE raw_ticks_bucketed ALTER COLUMN offsets_ms SET STORAGE EXTERNAL;")

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
    return size / 1024  # Returns size in KB


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
                # THE FIX: Bucket by the minute (second=0) to build massive arrays
                bucket_ts = ts.replace(second=0, microsecond=0)
                
                # THE FIX: Key by BOTH time and symbol
                bucket_key = (bucket_ts, symbol) 

                if bucket_key not in bucket:
                    bucket[bucket_key] = {
                        "prices": [],
                        "volumes": [],
                        "offsets": []
                    }

                offset = int((ts - bucket_ts).total_seconds() * 1000)
                bucket[bucket_key]["prices"].append(price)
                bucket[bucket_key]["volumes"].append(volume)
                bucket[bucket_key]["offsets"].append(offset)

            # ---- INSERT ----
            with conn.cursor() as cursor:
                for (bucket_ts, symbol), data in bucket.items(): 

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
                            symbol,
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
        print('\nInterrupted by user')
    finally:
        conn.close()

    return sizes


# ---------------- MAIN ---------------- #

def main():
    # Keep it running long enough to generate good data
    duration = 20
    speed = 5

    data_stream = list(generate_smart_batches(duration, speed))

    # -------- NO COMPRESSION --------
    print("\n--- WITHOUT COMPRESSION (True Raw Size) ---")
    reset_db()
    setup_bucket_table(compression=False)
    sizes_no_comp = ingest_and_measure(copy.deepcopy(data_stream))

    # -------- WITH COMPRESSION --------
    print("\n--- WITH LZ4 COMPRESSION ---")
    reset_db()
    setup_bucket_table(compression=True)
    sizes_comp = ingest_and_measure(copy.deepcopy(data_stream))

    # -------- PLOT --------
    plt.figure(figsize=(10, 6))

    plt.plot(sizes_no_comp, label="No Compression (EXTERNAL)", marker='o', color='red')
    plt.plot(sizes_comp, label="LZ4 Compression", marker='s', color='green')

    plt.title("Storage Growth: True Raw Size vs LZ4 Compression")
    plt.xlabel("Time (batches)")
    plt.ylabel("Size (KB)")
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()

    os.makedirs("./plots", exist_ok=True)
    plt.savefig("./plots/memory_compression_plot.png")
    
    print("\nBenchmark Complete. Displaying Plot...")
    plt.show()

if __name__ == "__main__":
    main()