import time
from datetime import datetime, timedelta
from connection import get_db_connection

SAFETY_OFFSET = timedelta(seconds=2) 

def rollup_to_1m_table(cursor, last_proc_ts):
    query = """
        INSERT INTO ohlcv_1m (bucket_time, symbol, open_price, high_price, low_price, close_price, total_volume)
        SELECT  
            date_trunc('minute', bucket_time) as minute_bucket,
            symbol,
            (ARRAY_AGG(open_price ORDER BY bucket_time ASC))[1],
            MAX(high_price),                                  
            MIN(low_price),                                   
            (ARRAY_AGG(close_price ORDER BY bucket_time DESC))[1],
            SUM(total_volume)
        FROM ohlcv_1s
        WHERE bucket_time >= date_trunc('minute', %s) 
          AND bucket_time < date_trunc('minute', %s) + interval '1 minute'
        GROUP BY symbol, minute_bucket
        ON CONFLICT (symbol, bucket_time) DO NOTHING;
    """
    cursor.execute(query, (last_proc_ts, last_proc_ts))
    print(f"--- [ROLLUP] Successfully summarized minute: {last_proc_ts.strftime('%H:%M')} ---")

def run_aggr_pipeline():
    conn = None 
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Start from where we left off
        cursor.execute("SELECT MAX(bucket_time) FROM ohlcv_1s")
        row = cursor.fetchone()
        
        if row[0]:
            last_proc_ts = row[0]
            print(f"Resuming aggregation from: {last_proc_ts}")
        else:
            last_proc_ts = datetime.now()
            print(f"No existing data. Starting from: {last_proc_ts}")

        while True:
            # wait a bit to let the generator push data
            time.sleep(0.5)
            curr_win_end = last_proc_ts + timedelta(seconds=1)
            
            if curr_win_end <= last_proc_ts:
                continue # Wait for virtual clock to move forward

            query = """
                SELECT 
                    date_trunc('second', ts) as bucket,
                    symbol,
                    (ARRAY_AGG(price ORDER BY ts ASC))[1] as open,
                    MAX(price) as high,
                    MIN(price) as low,
                    (ARRAY_AGG(price ORDER BY ts DESC))[1] as close,
                    SUM(volume) as vol
                FROM raw_ticks
                WHERE ts > %s AND ts <= %s
                GROUP BY symbol, bucket
                ORDER BY bucket ASC
            """
            cursor.execute(query, (last_proc_ts, curr_win_end))
            ohlcv_rows = cursor.fetchall()

            if ohlcv_rows:
                
                ins_query = """
                    INSERT INTO ohlcv_1s (bucket_time, symbol, open_price, high_price, low_price, close_price, total_volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (bucket_time, symbol) DO NOTHING
                """
                for row in ohlcv_rows:
                    cursor.execute(ins_query, row)
                
                # Check for 1-minute Rollup
                # If we've crossed into a new minute, trigger the 1m aggregation
                if last_proc_ts.minute != curr_win_end.minute:
                    rollup_to_1m_table(cursor, last_proc_ts) 
                
                last_proc_ts = curr_win_end
                conn.commit()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Aggregated up to {last_proc_ts.strftime('%H:%M:%S')}")

    except KeyboardInterrupt:
        print("\nAggregator stopped by user.")
    except Exception as e:
        print(f"Aggregator Error: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run_aggr_pipeline()