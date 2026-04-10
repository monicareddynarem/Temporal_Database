import psycopg2
import psycopg2.extras
import numpy as np
import time
from datetime import datetime, timedelta
from connection import get_db_connection

symbols_list = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

def insert_tick():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print('\n--- NUMPY SPEED DEMON STARTING ---')
        duration_minutes = int(input('Enter simulation duration in virtual minutes: '))
        n_speed = int(input('Enter speed multiplier (N virtual seconds per 1 real sec): '))
        
        start_v_time = datetime.now()
        end_v_time = start_v_time + timedelta(minutes=duration_minutes)
        virtual_time = start_v_time

        # 100 ticks per virtual second
        ticks_per_v_sec = 100
        ms_per_tick = 1000 / ticks_per_v_sec

        while virtual_time < end_v_time:
            real_loop_start = time.time()
            total_ticks = n_speed * ticks_per_v_sec
            
            # --- PHASE 1: NUMPY GENERATION ---
            gen_start = time.time()
            
            syms = np.random.choice(symbols_list, size=total_ticks)
            prices = np.round(np.random.uniform(100.0, 1500.0, size=total_ticks), 2)
            volumes = np.random.randint(1, 101, size=total_ticks)
            
            # Vectorized timestamp creation
            increments = np.arange(total_ticks) * timedelta(milliseconds=ms_per_tick)
            timestamps = virtual_time + increments
            
            # Convert to Python-friendly list of tuples
            batch = list(zip(syms.tolist(), prices.tolist(), volumes.tolist(), timestamps.tolist()))
            gen_time = time.time() - gen_start

            # --- PHASE 2: DATABASE INSERT ---
            db_start = time.time()
            sql = "INSERT INTO raw_ticks (symbol, price, volume, ts) VALUES %s"
            psycopg2.extras.execute_values(cursor, sql, batch)
            conn.commit()
            db_time = time.time() - db_start

            # Update virtual clock
            virtual_time += timedelta(seconds=n_speed)

            # --- DIAGNOSTICS ---
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ticks: {total_ticks} | Gen: {gen_time*1000:.1f}ms | DB: {db_time*1000:.1f}ms")

            # Wait to sync 1 real second
            elapsed = time.time() - real_loop_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            else:
                print(f"  ! LAGGING: Total loop took {elapsed:.2f}s")

    except KeyboardInterrupt:
        print('\nStopped by user')
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    insert_tick()