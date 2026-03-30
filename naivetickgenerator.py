import psycopg2
import psycopg2.extras
import random
import time
from datetime import datetime, timedelta
from connection import get_db_connection
    
symbols_list = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

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
        
        start_v_time = datetime.now()
        end_v_time = start_v_time + timedelta(minutes=duration_minutes)
        
        virtual_time = start_v_time
        ticks_per_v_sec = 100 * len(symbols_list)
        time_step = timedelta(milliseconds=1000 / ticks_per_v_sec)

        while virtual_time < end_v_time:
            real_start_time = time.time()
            batch = []
            
            total_ticks = n_speed * ticks_per_v_sec
            for _ in range(total_ticks):
                batch.append(generate_tick(virtual_time))
                virtual_time += time_step

            if batch:
                sql = "INSERT INTO raw_ticks (symbol, price, volume, ts) VALUES %s"
                psycopg2.extras.execute_values(cursor, sql, batch)
                conn.commit()
                
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Inserted {len(batch)} ticks | Virtual Clock: {virtual_time.strftime('%H:%M:%S')}")

            elapsed_real_time = time.time() - real_start_time
            time.sleep(max(0, 1.0 - elapsed_real_time))

    except KeyboardInterrupt:
        print('\nInterrupted by user')
    except Exception as e:
        print(f"DB error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    insert_tick()