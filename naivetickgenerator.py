import psycopg2
import psycopg2.extras
import random
import time
from datetime import datetime, timedelta
from connection import get_db_connection
    
symbols_list = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

def ensure_partition(cursor, ts, table='raw_ticks'):
    """Ensures the partition exists before naivetickgenerator attempts insertion."""
    date_str = ts.strftime('%Y_%m_%d')
    start_str = ts.strftime('%Y-%m-%d 00:00:00')
    end_str = (ts + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
    p_name = f"{table}_{date_str}"
    
    cursor.execute("SELECT 1 FROM pg_class WHERE relname = %s", (p_name,))
    if not cursor.fetchone():
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {p_name} PARTITION OF {table} FOR VALUES FROM ('{start_str}') TO ('{end_str}')")

def generate_tick(virtual_time):
    sym = random.choice(symbols_list)
    price = round(random.uniform(100.0, 1500.0), 2)
    volume = random.randint(1, 100)
    return (sym, price, volume, virtual_time)

def insert_tick():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        print('Tick generator started... Press Ctrl+C to stop')
        duration_minutes = int(input('Enter simulation duration in virtual minutes: '))
        n_speed = int(input('Enter speed multiplier (N virtual seconds per 1 real sec): '))
        
        virtual_time = datetime.now()
        end_v_time = virtual_time + timedelta(minutes=duration_minutes)
        ticks_per_v_sec = 100 * len(symbols_list)
        time_step = timedelta(milliseconds=1000 / ticks_per_v_sec)

        while virtual_time < end_v_time:
            real_start_time = time.time()
            batch = []
            
            # Ensure partition exists for this specific batch's timestamp
            ensure_partition(cursor, virtual_time)
            
            total_ticks = n_speed * ticks_per_v_sec
            for _ in range(total_ticks):
                batch.append(generate_tick(virtual_time))
                virtual_time += time_step

            if batch:
                sql = "INSERT INTO raw_ticks (symbol, price, volume, ts) VALUES %s"
                psycopg2.extras.execute_values(cursor, sql, batch)
                conn.commit()
                
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Inserted {len(batch)} ticks | V-Clock: {virtual_time.strftime('%H:%M:%S')}")

            elapsed = time.time() - real_start_time
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)

    except KeyboardInterrupt:
        print('\nStopped by user.')
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    insert_tick()