import psycopg2
import numpy as np
import time
import io
from datetime import datetime, timedelta

# Symbols to pick from
symbols_list = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']


def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="23CS10059",
            user="23CS10059",
            password="ashok@123",
            host="10.5.18.102",
            port="5432"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

        
def insert_tick():
    conn = None
    try:
        conn = get_db_connection()
        # Optimization: Disable synchronous commit for raw speed
        with conn.cursor() as setup_cursor:
            setup_cursor.execute("SET synchronous_commit = OFF;")
        
        print('\n--- ULTIMATE SPEED DEMON (COPY METHOD) ---')
        duration_min = int(input('Simulation duration (virtual minutes): '))
        n_speed = int(input('Speed multiplier (N virtual sec / 1 real sec): '))
        
        # 1000 ticks per virtual second (as you requested)
        ticks_per_v_sec = 100 * len(symbols_list)
        ms_per_tick = 1000 / ticks_per_v_sec
        
        virtual_time = datetime.now()
        end_v_time = virtual_time + timedelta(minutes=duration_min)

        while virtual_time < end_v_time:
            real_loop_start = time.time()
            total_ticks = n_speed * ticks_per_v_sec
            
            # --- PHASE 1: VECTORIZED DATA GENERATION (NumPy) ---
            gen_start = time.time()
            syms = np.random.choice(symbols_list, size=total_ticks)
            prices = np.round(np.random.uniform(100.0, 1500.0, size=total_ticks), 2)
            volumes = np.random.randint(1, 101, size=total_ticks)
            
            # Create timestamp array
            increments = np.arange(total_ticks) * timedelta(milliseconds=ms_per_tick)
            timestamps = virtual_time + increments
            gen_time = time.time() - gen_start

            # --- PHASE 2: BUFFER CREATION (CSV Format in Memory) ---
            buf_start = time.time()
            f = io.StringIO()
            # Efficiently write to buffer as Tab-Separated Values (TSV)
            for i in range(total_ticks):
                f.write(f"{syms[i]}\t{prices[i]}\t{volumes[i]}\t{timestamps[i]}\n")
            f.seek(0)
            buf_time = time.time() - buf_start

            # --- PHASE 3: STREAMING COPY TO DB ---
            db_start = time.time()
            with conn.cursor() as cursor:
                # copy_from is significantly faster than any INSERT statement
                cursor.copy_from(f, 'raw_ticks', columns=('symbol', 'price', 'volume', 'ts'))
            conn.commit()
            db_time = time.time() - db_start

            # Update Virtual Clock
            virtual_time += timedelta(seconds=n_speed)

            # Diagnostics
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Rows: {total_ticks} | Gen: {gen_time*1000:.1f}ms | Buf: {buf_time*1000:.1f}ms | DB: {db_time*1000:.1f}ms")

            # Maintain 1-second real-time pacing
            elapsed = time.time() - real_loop_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            else:
                print(f"  !! LAGGING: Loop took {elapsed:.2f}s")

    except KeyboardInterrupt:
        print('\nStopped by user.')
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    insert_tick()