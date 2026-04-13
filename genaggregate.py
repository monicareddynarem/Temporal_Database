import time
from datetime import datetime, timedelta
from utils.connection import get_db_connection

symbols = ['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC']

def ensure_partition(cursor, ts, table):
    """Pre-creates the daily partition to bypass PostgreSQL routing limits."""
    date_str = ts.strftime('%Y_%m_%d')
    start_str = ts.strftime('%Y-%m-%d 00:00:00')
    end_str = (ts + timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
    p_name = f"{table}_{date_str}"
    
    cursor.execute("SELECT 1 FROM pg_class WHERE relname = %s", (p_name,))
    if not cursor.fetchone():
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {p_name} PARTITION OF {table} FOR VALUES FROM ('{start_str}') TO ('{end_str}')")
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_look_{p_name} ON {p_name} (symbol, ts_bucket DESC)")

def rollup_to_1m_table(cursor, last_proc_ts):
    ensure_partition(cursor, last_proc_ts, 'ohlcv_1m')
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
        WHERE ts_bucket >= date_trunc('minute', %s) 
          AND ts_bucket < date_trunc('minute', %s) + interval '1 minute'
        GROUP BY symbol, minute_bucket
        ON CONFLICT (ts_bucket, symbol) DO NOTHING;
    """
    cursor.execute(query, (last_proc_ts, last_proc_ts))
    print(f"\t--- [ROLLUP] Minute {last_proc_ts.strftime('%H:%M')} finalized ---")

def run_aggr_pipeline():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        now = datetime.now()
        
        cursor.execute("SELECT last_processed_ts FROM aggregation_watermarks WHERE aggregation_interval = '1s'")
        row = cursor.fetchone()
        last_proc_ts = row[0].replace(microsecond=0) if row else (datetime(2026, 3, 13, 9, 0, 0))
        last_close = {}
        ins_query = """
            INSERT INTO ohlcv_1s (ts_bucket, symbol, open_price, high_price, low_price, close_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ts_bucket, symbol) DO NOTHING
        """

        while True:
            time.sleep(1)
            query = """ 
                SELECT date_trunc('second', ts) as bucket, symbol, 
                (ARRAY_AGG(price ORDER BY ts ASC))[1], MAX(price), MIN(price), 
                (ARRAY_AGG(price ORDER BY ts DESC))[1], SUM(volume) FROM raw_ticks WHERE ts >= %s AND ts < %s 
                GROUP BY symbol, bucket 
                ORDER BY bucket ASC 
            """
            curr_win_end = (last_proc_ts + timedelta(seconds=1)).replace(microsecond=0)

            cursor.execute(query, (last_proc_ts, curr_win_end))
            rows = cursor.fetchall()

            present_symbols = set(r[1] for r in rows)
            missing_symbols = set(symbols) - present_symbols

            ensure_partition(cursor, last_proc_ts, 'ohlcv_1s')

            # insert real rows
            for r in rows:
                last_close[r[1]] = r[5]
                cursor.execute(ins_query, r)

            # fill missing
            for symbol in missing_symbols:
                if symbol in last_close:
                    price = last_close[symbol]
                    fill_row = (curr_win_end, symbol, price, price, price, price, 0)
                    cursor.execute(ins_query, fill_row)

            if rows:
                print(f"--- [ROLLUP] Second {last_proc_ts.strftime('%H:%M:%S')} finalized ---")
            else:
                print(f"[GAP FILLED] {last_proc_ts.strftime('%H:%M:%S')}")

            if curr_win_end.second == 0:
                rollup_to_1m_table(cursor, curr_win_end)


            last_proc_ts = curr_win_end

            cursor.execute("""
                UPDATE aggregation_watermarks 
                SET last_processed_ts = %s 
                WHERE aggregation_interval = '1s'
            """, (last_proc_ts,))

            conn.commit()

    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        if conn: 
            conn.close()

if __name__ == "__main__":
    run_aggr_pipeline()