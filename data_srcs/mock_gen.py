import numpy as np
import time
from datetime import datetime, timedelta

symbols_list = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

def generate_naive_batches(duration_minutes, n_speed, ticks_per_v_sec=100):
    """
    Generates simulated tick data using NumPy and yields it in batches.
    Format: list of tuples (symbol, price, volume, ts)
    """
    virtual_time = datetime.now()
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
        
        # --- THE FIX IS HERE ---
        # Convert the float timestamps into actual Python datetime objects
        dt_timestamps = [datetime.fromtimestamp(ts) for ts in timestamps.tolist()]
        
        # Use dt_timestamps instead of timestamps.tolist()
        batch = list(zip(syms.tolist(), prices.tolist(), volumes.tolist(), dt_timestamps))
        gen_time = time.time() - gen_start
        
        yield batch, virtual_time, gen_time, total_ticks
        
        # Update virtual clock for the next loop
        virtual_time += timedelta(seconds=n_speed)