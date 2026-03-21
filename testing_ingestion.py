import psycopg2
import psycopg2.extras 
import numpy as np
import random
import time
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# 1. CONFIGURATION
DB_CONFIG = {
    "dbname": "23CS10059",
    "user": "23CS10059",
    "password": "ashok@123",
    "host": "10.5.18.102",
    "port": "5432"
}

symbols_list = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

# 2. BENCHMARK FUNCTIONS

def bench_row_wise(cursor, total_ticks):
    t0 = time.time()
    sql = "INSERT INTO raw_ticks (symbol, price, volume, ts) VALUES (%s, %s, %s, %s)"
    now = datetime.now()
    for i in range(total_ticks):
        tick = (random.choice(symbols_list), 100.0, 10, now)
        cursor.execute(sql, tick)
    return time.time() - t0

def bench_batch_list(cursor, total_ticks):
    now = datetime.now()
    batch = [(random.choice(symbols_list), 100.0, 10, now) for _ in range(total_ticks)]
    sql = "INSERT INTO raw_ticks (symbol, price, volume, ts) VALUES %s"
    t0 = time.time()
    psycopg2.extras.execute_values(cursor, sql, batch)
    return time.time() - t0

def bench_batch_numpy(cursor, total_ticks):
    now = datetime.now()
    syms = np.random.choice(symbols_list, size=total_ticks)
    prices = np.random.uniform(100.0, 1500.0, size=total_ticks)
    volumes = np.random.randint(1, 101, size=total_ticks)
    batch = list(zip(syms.tolist(), prices.tolist(), volumes.tolist(), [now]*total_ticks))
    
    sql = "INSERT INTO raw_ticks (symbol, price, volume, ts) VALUES %s"
    t0 = time.time()
    psycopg2.extras.execute_values(cursor, sql, batch)
    return time.time() - t0

# 3. RUN BENCHMARK & PLOT

def run_simulation():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    n_range = [1, 5, 10, 20, 40, 60, 80, 100,120]
    
    results_row = []
    results_list = []
    results_numpy = []

    print("Starting Stress Test Sweep...")

    for n in n_range:
        total_ticks = n * 100 * len(symbols_list)
        print(f"Testing N={n} ({total_ticks} rows)...")
        
        # Test Row-Wise
        t_row = bench_row_wise(cursor, total_ticks)
        conn.rollback()
        results_row.append(t_row)
        
        # Test Batch List
        t_list = bench_batch_list(cursor, total_ticks)
        conn.rollback()
        results_list.append(t_list)
        
        # Test Batch NumPy
        t_numpy = bench_batch_numpy(cursor, total_ticks)
        conn.rollback()
        results_numpy.append(t_numpy)

    conn.close()

    # PLOTTING 
    plt.figure(figsize=(10, 6))
    plt.plot(n_range, results_row, label='Row-Wise (Slow)', marker='o', color='red')
    plt.plot(n_range, results_list, label='Batch (List)', marker='s', color='orange')
    plt.plot(n_range, results_numpy, label='Batch (NumPy)', marker='^', color='green')

    plt.title('PostgreSQL Ingestion Speed: Row-Wise vs Batching')
    plt.xlabel('N (Speed Multiplier - Virtual Seconds)')
    plt.ylabel('Time Taken (Seconds)')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()
    
    plt.axhline(y=1.0, color='blue', linestyle=':', label='1s Real-Time Limit')
    
    print("\nBenchmark Complete. Displaying Plot...")
    plt.show()

if __name__ == "__main__":
    run_simulation()