import time
import psycopg2.extras
from datetime import datetime, timedelta
from utils.connection import get_db_connection

def ensure_partition(cursor, ts, table='raw_ticks'):
    date_str = ts.strftime('%Y_%m_%d')
    start_str = ts.strftime('%Y-%m-%d 00:00:00')
    end_str = (ts + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
    p_name = f"{table}_{date_str}"
    
    cursor.execute("SELECT 1 FROM pg_class WHERE relname = %s", (p_name,))
    if not cursor.fetchone():
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {p_name} PARTITION OF {table} FOR VALUES FROM ('{start_str}') TO ('{end_str}')")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_ts_{p_name} ON {p_name} (symbol, ts)")
    return p_name

def ingest_batch_list(data_generator):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET synchronous_commit = OFF;")
        conn.commit()

        print('\n--- DB INGESTER: BATCH LIST (EXECUTE_VALUES) ACTIVE ---')

        # FIX: Correctly unpack the 4 values
        for batch_list, current_v_time, gen_time, total_ticks in data_generator:
            real_start = time.time()
            
            with conn.cursor() as cursor:
                db_start = time.time()
                target_table = ensure_partition(cursor, current_v_time)
                # Use target_table instead of raw_ticks
                sql = f"INSERT INTO {target_table} (symbol, price, volume, ts) VALUES %s"
                psycopg2.extras.execute_values(cursor, sql, batch_list)
            conn.commit()
            db_time = time.time() - db_start

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Ticks: {total_ticks} | Gen: {gen_time*1000:.1f}ms | DB Write: {db_time*1000:.1f}ms | V-Clock: {current_v_time.strftime('%H:%M:%S')}")

            elapsed = time.time() - real_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            else:
                print(f"  ! LAGGING: Total loop took {elapsed:.2f}s")
                
    finally:
        if conn: conn.close()