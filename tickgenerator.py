import psycopg2
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
        start_v_time = datetime.now()
        end_v_time = start_v_time + timedelta(minutes=duration_minutes)
        
        virtual_time = start_v_time

        while virtual_time < end_v_time:
            tick = generate_tick(virtual_time)
            
            sql = "INSERT INTO raw_ticks (symbol, price, volume, ts) VALUES (%s, %s, %s, %s)" 

            cursor.execute(sql, tick)
            conn.commit()
            
            print(f"Inserted Entry: {tick}")

            virtual_time += timedelta(milliseconds=1)

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