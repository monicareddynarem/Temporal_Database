import psycopg2
import psycopg2.extras
import numpy as np
import time
from datetime import datetime, timedelta
from connection import get_db_connection

# Shared configuration
symbols_list = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

def ensure_partition(cursor, ts, table='raw_ticks'):
    """Requirement: Ensures the daily partition exists for the partitioned raw_ticks table."""
    date_str = ts.strftime('%Y_%m_%d')
    start_str = ts.strftime('%Y-%m-%d 00:00:00')
    end_str = (ts + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
    p_name = f"{table}_{date_str}"
    
    cursor.execute("SELECT 1 FROM pg_class WHERE relname = %s", (p_name,))
    if not cursor.fetchone():
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {p_name} PARTITION OF {table} FOR VALUES FROM ('{start_str}') TO ('{end_str}')")
        # Ensure index for faster lookups later
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_ts_{p_name} ON {p_name} (symbol, ts)")

def insert_tick():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print('\n--- LIST GENERATOR STARTING ---')
        duration_minutes = int(input('Enter simulation duration in virtual minutes: '))
        n_speed = int(input('Enter speed multiplier (N virtual seconds per 1 real sec): '))
        
        virtual_time = datetime.now()
        end_v_time = virtual_time + timedelta(minutes=duration_minutes)

        # 100 ticks per virtual second
        ticks_per_v_sec = 100
        ms_per_tick = 1000 / ticks_per_v_sec

        while virtual_time < end_v_time:
            real_loop_start = time.time()
            total_ticks = n_speed * ticks_per_v_sec
            
            # Ensure the database has a home for these timestamps
            ensure_partition(cursor, virtual_time)

            gen_start = time.time()
            
            syms = np.random.choice(symbols_list, size=total_ticks)
            prices = np.round(np.random.uniform(100.0, 1500.0, size=total_ticks), 2)
            volumes = np.random.randint(1, 101, size=total_ticks)
            
            # Vectorized timestamp creation for all ticks in this second
            increments = np.arange(total_ticks) * timedelta(milliseconds=ms_per_tick)
            timestamps = virtual_time + increments
            
            # Prepare batch for execute_values
            batch = list(zip(syms.tolist(), prices.tolist(), volumes.tolist(), timestamps.tolist()))
            gen_time = time.time() - gen_start

            db_start = time.time()
            sql = "INSERT INTO raw_ticks (symbol, price, volume, ts) VALUES %s"
            psycopg2.extras.execute_values(cursor, sql, batch)
            conn.commit()
            db_time = time.time() - db_start

            # Update virtual clock
            virtual_time += timedelta(seconds=n_speed)

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ticks: {total_ticks} | Gen: {gen_time*1000:.1f}ms | DB: {db_time*1000:.1f}ms | V-Clock: {virtual_time.strftime('%H:%M:%S')}")

            # Maintain real-time pacing
            elapsed = time.time() - real_loop_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            else:
                print(f"  ! LAGGING: Total loop took {elapsed:.2f}s")

    except KeyboardInterrupt:
        print('\nStopped by user.')
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    insert_tick()