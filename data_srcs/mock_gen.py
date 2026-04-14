import numpy as np
import time
from datetime import datetime, timedelta
from utils.connection import get_db_connection

symbols_list = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

def generate_naive_batches(duration_minutes, n_speed, ticks_per_v_sec=100):
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT last_processed_ts FROM aggregation_watermarks WHERE aggregation_interval = '1s'")
            row = cursor.fetchone()
            if row:
                virtual_time = row[0].replace(tzinfo=None)
                print(f"\n[RESUME] Found existing DB watermark. Resuming NumPy generation from {virtual_time}")
            else:
                virtual_time = datetime(2024, 4, 11, 14, 0, 0)
                print(f"\n[START] Clean database detected. Starting fresh from {virtual_time}")
    except Exception:
        virtual_time = datetime(2024, 4, 11, 14, 0, 0)
    finally:
        if 'conn' in locals() and conn:
            conn.close()

    end_v_time = virtual_time + timedelta(minutes=duration_minutes)
    ms_per_tick = 1000 / ticks_per_v_sec

    while virtual_time < end_v_time:
        gen_start = time.time()
        total_ticks = n_speed * ticks_per_v_sec
        
        syms = np.random.choice(symbols_list, size=total_ticks)
        prices = np.round(np.random.uniform(100.0, 1500.0, size=total_ticks), 2)
        volumes = np.random.randint(1, 101, size=total_ticks)
                
        base_ts = virtual_time.timestamp()
        ts_step = ms_per_tick / 1000.0
        timestamps = np.arange(total_ticks) * ts_step + base_ts
        
        dt_timestamps = [datetime.fromtimestamp(ts) for ts in timestamps.tolist()]
        batch = list(zip(syms.tolist(), prices.tolist(), volumes.tolist(), dt_timestamps))
        gen_time = time.time() - gen_start
        
        yield batch, virtual_time, gen_time, total_ticks
        
        virtual_time += timedelta(seconds=n_speed)