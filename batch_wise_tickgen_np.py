import psycopg2
import numpy as np
import time
import io
from datetime import datetime, timedelta
from connection import get_db_connection

symbols_list = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

def ensure_partition(cursor, ts, table):
    """Pre-creates the daily partition to bypass PostgreSQL's COPY routing limitations."""
    date_str = ts.strftime('%Y_%m_%d')
    start_str = ts.strftime('%Y-%m-%d 00:00:00')
    end_str = (ts + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
    p_name = f"{table}_{date_str}"
    
    cursor.execute("SELECT 1 FROM pg_class WHERE relname = %s", (p_name,))
    if not cursor.fetchone():
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {p_name} PARTITION OF {table} FOR VALUES FROM ('{start_str}') TO ('{end_str}')")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_ts_{p_name} ON {p_name} (symbol, ts)")

def setup_database(conn):
    with conn.cursor() as cursor:
        cursor.execute("SET synchronous_commit = OFF;")
    conn.commit()

def insert_tick():
    conn = None
    try:
        conn = get_db_connection()
        setup_database(conn)
        
        print('\n--- VECTORIZED INGESTION ACTIVE ---')
        duration_min = int(input('Simulation duration (virtual minutes): '))
        n_speed = int(input('Speed multiplier (N virtual sec / 1 real sec): '))
        
        ticks_per_v_sec = 1000 
        ms_per_tick = 1000.0 / ticks_per_v_sec
        virtual_time = datetime.now()
        end_v_time = virtual_time + timedelta(minutes=duration_min)

        while virtual_time < end_v_time:
            real_start = time.time()
            total_ticks = n_speed * ticks_per_v_sec
            
            syms = np.random.choice(symbols_list, size=total_ticks)
            prices = np.round(np.random.uniform(100.0, 1500.0, size=total_ticks), 2)
            volumes = np.random.randint(1, 101, size=total_ticks)
            
            base_ts = virtual_time.timestamp()
            ts_step = ms_per_tick / 1000.0
            timestamps = np.arange(total_ticks) * ts_step + base_ts
            
            lines = [
                f"{syms[i]}\t{prices[i]:.2f}\t{volumes[i]}\t{datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d %H:%M:%S.%f')}" 
                for i in range(total_ticks)
            ]
            
            f = io.StringIO('\n'.join(lines) + '\n')
            with conn.cursor() as cursor:
                # CRITICAL FIX: Ensure partition exists right before COPY
                ensure_partition(cursor, virtual_time, 'raw_ticks')
                cursor.copy_from(f, 'raw_ticks', sep='\t', columns=('symbol', 'price', 'volume', 'ts'))
            conn.commit()

            virtual_time += timedelta(seconds=n_speed)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ingested {total_ticks} rows | V-Clock: {virtual_time.strftime('%H:%M:%S')}")

            elapsed = time.time() - real_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)

    except KeyboardInterrupt:
        print('\nStopped by user.')
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    insert_tick()