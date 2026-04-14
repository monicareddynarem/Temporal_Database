import time
import io
import pandas as pd
from datetime import datetime, timedelta
from utils.connection import get_db_connection

def ensure_partition(cursor, ts, table='raw_ticks_bucketed'):
<<<<<<< HEAD
    
=======
>>>>>>> origin/master
    date_str = ts.strftime('%Y_%m_%d')
    start_str = ts.strftime('%Y-%m-%d 00:00:00')
    end_str = (ts + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
    p_name = f"{table}_{date_str}"
    
    cursor.execute("SELECT 1 FROM pg_class WHERE relname = %s", (p_name,))
    if not cursor.fetchone():
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {p_name} PARTITION OF {table} FOR VALUES FROM ('{start_str}') TO ('{end_str}')")
<<<<<<< HEAD
        
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_bucket_{p_name} ON {p_name} (symbol, bucket_ts)")

def ingest_compressed_arrays(data_generator):
    
=======
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_bucket_{p_name} ON {p_name} (symbol, bucket_ts)")

def ingest_compressed_arrays(data_generator):
>>>>>>> origin/master
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET synchronous_commit = OFF;")
        conn.commit()

        print('\n--- DB INGESTER: COMPRESSED ARRAY BUCKETING ACTIVE ---')

        for batch_list, current_v_time, gen_time, total_ticks in data_generator:
            real_start = time.time()
            
<<<<<<< HEAD
            
            df = pd.DataFrame(batch_list, columns=['symbol', 'price', 'volume', 'ts'])
            df['ts'] = pd.to_datetime(df['ts'])

            
            # Truncate to the nearest second
=======
            df = pd.DataFrame(batch_list, columns=['symbol', 'price', 'volume', 'ts'])
            df['ts'] = pd.to_datetime(df['ts'])

>>>>>>> origin/master
            df['bucket_ts'] = df['ts'].dt.floor('s')
            df['offset_ms'] = ((df['ts'] - df['bucket_ts']).dt.total_seconds() * 1000).astype(int)

<<<<<<< HEAD
            
=======
>>>>>>> origin/master
            bucketed = df.groupby(['bucket_ts', 'symbol']).agg({
                'price': lambda x: '{' + ','.join(x.astype(str)) + '}',
                'volume': lambda x: '{' + ','.join(x.astype(str)) + '}',
                'offset_ms': lambda x: '{' + ','.join(x.astype(str)) + '}'
            }).reset_index()
            
<<<<<<< HEAD
            # Buffer to memory as TSV
=======
>>>>>>> origin/master
            f = io.StringIO()
            bucketed.to_csv(f, sep='\t', header=False, index=False, quoting=3) 
            f.seek(0)
            
            with conn.cursor() as cursor:
                ensure_partition(cursor, current_v_time)
                
                db_start = time.time()
                cursor.copy_from(f, 'raw_ticks_bucketed', sep='\t', 
                                 columns=('bucket_ts', 'symbol', 'prices', 'volumes', 'offsets_ms'))
            conn.commit()
            db_time = time.time() - db_start

            print(f"[IST:{datetime.now().strftime('%H:%M:%S')}] Raw Ticks: {total_ticks} -> DB Rows: {len(bucketed)} | Gen: {gen_time*1000:.1f}ms | DB Write: {db_time*1000:.1f}ms | V-Clock: {current_v_time.strftime('%H:%M:%S')}")

            elapsed = time.time() - real_start
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            else:
                print(f"  ! LAGGING: Total loop took {elapsed:.2f}s")
                
    finally:
        if conn: conn.close()