import time
import sys
from datetime import datetime, timedelta
from utils.connection import get_db_connection

symbols = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

def ensure_partition(cursor, ts, table):
    date_str = ts.strftime('%Y_%m_%d')
    start_str = ts.strftime('%Y-%m-%d 00:00:00')
    end_str = (ts + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
    p_name = f"{table}_{date_str}"
    
    cursor.execute("SELECT 1 FROM pg_class WHERE relname = %s", (p_name,))
    if not cursor.fetchone():
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {p_name} PARTITION OF {table} FOR VALUES FROM ('{start_str}') TO ('{end_str}')")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_look_{p_name} ON {p_name} (symbol, ts_bucket DESC)")

def rollup_to_1m_table(cursor, current_ts):
    
    # If the clock just hit 14:01:00, subtract 1 second (14:00:59) 
    # so we correctly target and roll up the 14:00:xx minute bucket
    target_minute = current_ts - timedelta(seconds=1)
    
    ensure_partition(cursor, target_minute, 'ohlcv_1m')
    query = """
        INSERT INTO ohlcv_1m (ts_bucket, symbol, open_price, high_price, low_price, close_price, volume)
        SELECT  
            date_trunc('minute', ts_bucket) as minute_bucket,
            symbol,
            (ARRAY_AGG(open_price ORDER BY ts_bucket ASC))[1],
            MAX(high_price),
            MIN(low_price),
            (ARRAY_AGG(close_price ORDER BY ts_bucket DESC))[1],
            SUM(volume)
        FROM ohlcv_1s
        WHERE ts_bucket >= date_trunc('minute', %s::timestamp) 
          AND ts_bucket < date_trunc('minute', %s::timestamp) + interval '1 minute'
        GROUP BY symbol, minute_bucket
        ON CONFLICT (ts_bucket, symbol) DO NOTHING;
    """
    # Pass the target_minute instead of current_ts
    cursor.execute(query, (target_minute, target_minute))
    
    minute_str = target_minute.strftime('%H:%M')
    print(f"\n{'='*50}")
    print(f"  MINUTE ROLLUP COMPLETE: {minute_str}")
    print(f" Squashed 60 seconds of data into 1-Minute Candles")
    print(f"{'='*50}\n")

    
def run_aggr_pipeline():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        default_start = datetime(2024, 4, 11, 14, 0, 0)
        
        try:
            cursor.execute("SELECT last_processed_ts FROM aggregation_watermarks WHERE aggregation_interval = '1s'")
            row = cursor.fetchone()
            last_proc_ts = row[0].replace(microsecond=0) if row else default_start
        except Exception:
            last_proc_ts = default_start
            
        conn.commit()

        ins_query = """
            INSERT INTO ohlcv_1s (ts_bucket, symbol, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ts_bucket, symbol) DO NOTHING
        """

        while True:
            time.sleep(1)
            
            
            try:
                cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'aggregation_watermarks');")
                if cursor.fetchone()[0]:
                    cursor.execute("SELECT last_processed_ts FROM aggregation_watermarks WHERE aggregation_interval = '1s'")
                    wm_row = cursor.fetchone()
                    
                    
                    if not wm_row and last_proc_ts > default_start:
                        last_proc_ts = default_start
                        sys.stdout.write("\r" + " " * 80 + "\r") 
                        print("\n Database wipe detected (empty watermarks)! Resetting aggregator to 14:00:00...")
                        conn.commit()
                    
                    
                    elif wm_row and wm_row[0] < last_proc_ts:
                        last_proc_ts = wm_row[0]
                        sys.stdout.write("\r" + " " * 80 + "\r")
                        print(f"\n Database reset detected! Rewinding aggregator to {last_proc_ts.strftime('%H:%M:%S')}...")
                        conn.commit()
            except Exception:
                
                conn.rollback()
                continue
            
            
            cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'raw_ticks_bucketed');")
            is_compressed = cursor.fetchone()[0]

            if is_compressed:
                cursor.execute("SELECT MAX(bucket_ts) FROM raw_ticks_bucketed")
            else:
                cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'raw_ticks');")
                if cursor.fetchone()[0]:
                    cursor.execute("SELECT MAX(ts) FROM raw_ticks")
                else:
                    conn.rollback() 
                    sys.stdout.write(f"\r[WAITING] No raw tables found yet. Waiting for ingester... ")
                    sys.stdout.flush()
                    continue
                
            max_ingested_ts = cursor.fetchone()[0]

            if not max_ingested_ts or last_proc_ts >= max_ingested_ts:
                conn.rollback() 
                sys.stdout.write(f"\r[WAITING] Aggregator is at {last_proc_ts.strftime('%H:%M:%S')}. Waiting for new trades... ")
                sys.stdout.flush()
                continue
            else:
                sys.stdout.write("\r" + " " * 80 + "\r")
                sys.stdout.flush()
            
            curr_win_end = (last_proc_ts + timedelta(seconds=1)).replace(microsecond=0)

            if is_compressed:
                query = """ 
                    SELECT 
                        bucket_ts as bucket, symbol, prices[1], 
                        (SELECT MAX(p) FROM unnest(prices) p), 
                        (SELECT MIN(p) FROM unnest(prices) p), 
                        prices[array_length(prices, 1)], 
                        (SELECT SUM(v) FROM unnest(volumes) v),
                        array_length(prices, 1) as tick_count
                    FROM raw_ticks_bucketed 
                    WHERE bucket_ts = %s 
                """
                cursor.execute(query, (last_proc_ts,))
            else:
                query = """ 
                    SELECT date_trunc('second', ts) as bucket, symbol, 
                    (ARRAY_AGG(price ORDER BY ts ASC))[1], MAX(price), MIN(price), 
                    (ARRAY_AGG(price ORDER BY ts DESC))[1], SUM(volume),
                    COUNT(*) as tick_count
                    FROM raw_ticks WHERE ts >= %s AND ts < %s 
                    GROUP BY symbol, bucket 
                    ORDER BY bucket ASC 
                """
                cursor.execute(query, (last_proc_ts, curr_win_end))

            rows = cursor.fetchall()
            ensure_partition(cursor, last_proc_ts, 'ohlcv_1s')

            total_ticks_in_second = 0

            for r in rows:
                if r[7] is not None:
                    total_ticks_in_second += r[7]
                cursor.execute(ins_query, r[:7]) 

            time_str = last_proc_ts.strftime('%H:%M:%S')
            if rows:
                active_symbols = len(rows)
                print(f"[1s ROLLUP] {time_str} | Processed {total_ticks_in_second:,} trades across {active_symbols} symbols")

            if curr_win_end.second == 0:
                rollup_to_1m_table(cursor, curr_win_end)

            last_proc_ts = curr_win_end

            cursor.execute("""
                INSERT INTO aggregation_watermarks (aggregation_interval, last_processed_ts)
                VALUES ('1s', %s)
                ON CONFLICT (aggregation_interval) DO UPDATE SET last_processed_ts = EXCLUDED.last_processed_ts
            """, (last_proc_ts,))

            conn.commit()

    except KeyboardInterrupt:
        print("\n Aggregator stopped.")
    except Exception as e:
        print(f"\n Aggregator crashed: {e}")
    finally:
        if conn: 
            conn.close()

if __name__ == "__main__":
    run_aggr_pipeline()