import psycopg2
import numpy as np
import time
import io
from datetime import datetime, timedelta
from connection import get_db_connection

symbols_list = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

def setup_database(conn):
    """Ensures the table exists, has correct types, and is optimized for speed."""
    with conn.cursor() as cursor:

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_ticks (
                symbol VARCHAR(10),
                price NUMERIC(10, 2),
                volume INTEGER,
                ts TIMESTAMP
            );
        """)
        # 2. You cannot make the parent partitioned table UNLOGGED
        # But you can make each individual partition UNLOGGED
        # cursor.execute("ALTER TABLE raw_ticks SET UNLOGGED;")

        # 3. Session-level speed boosts
        cursor.execute("SET synchronous_commit = OFF;")
    conn.commit()

def insert_tick():
    conn = None
    try:
        conn = get_db_connection()
        setup_database(conn)
        
        print('\n--- FINAL VECTORIZED SPEED DEMON ---')
        duration_min = int(input('Simulation duration (virtual minutes): '))
        n_speed = int(input('Speed multiplier (N virtual sec / 1 real sec): '))
        
        ticks_per_v_sec = 1000 
        ms_per_tick = 1000.0 / ticks_per_v_sec
        
        virtual_time = datetime.now()
        end_v_time = virtual_time + timedelta(minutes=duration_min)

        while virtual_time < end_v_time:
            real_start = time.time()
            total_ticks = n_speed * ticks_per_v_sec
            
            # PHASE 1: VECTORIZED GENERATION
            gen_start = time.time()
            syms = np.random.choice(symbols_list, size=total_ticks)
            prices = np.round(np.random.uniform(100.0, 1500.0, size=total_ticks), 2)
            volumes = np.random.randint(1, 101, size=total_ticks)
            gen_time = time.time() - gen_start

            #  PHASE 2: HIGH-SPEED STRING BUFFERING 
            buf_start = time.time()
            
            # Generate accurate timestamps for every single tick
            base_ts = virtual_time.timestamp()
            ts_step = ms_per_tick / 1000.0
            timestamps = np.arange(total_ticks) * ts_step + base_ts
            
            # Python's '\n'.join is executed natively in C. 
            # This avoids all NumPy dtype formatting errors.
            lines = [
                f"{syms[i]}\t{prices[i]:.2f}\t{volumes[i]}\t{datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d %H:%M:%S.%f')}" 
                for i in range(total_ticks)
            ]
            
            f = io.StringIO('\n'.join(lines) + '\n')
            buf_time = time.time() - buf_start

            #  PHASE 3: STREAMING COPY 
            db_start = time.time()
            with conn.cursor() as cursor:
                cursor.copy_from(f, 'raw_ticks', sep='\t', columns=('symbol', 'price', 'volume', 'ts'))
            conn.commit()
            db_time = time.time() - db_start

            # Update virtual clock
            virtual_time += timedelta(seconds=n_speed)
            
            # Final Stats
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Rows: {total_ticks} | Gen: {gen_time*1000:.0f}ms | Buf: {buf_time*1000:.0f}ms | DB: {db_time*1000:.0f}ms")

            # Maintain 1-second real-time pacing
            elapsed = time.time() - real_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            else:
                print(f"  !! HARDWARE LIMIT REACHED: Loop took {elapsed:.2f}s")

    except KeyboardInterrupt:
        print('\nStopped by user.')
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    insert_tick()