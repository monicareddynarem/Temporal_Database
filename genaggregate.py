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
    print(f"--- [ROLLUP] Minute done: {last_proc_ts.strftime('%H:%M')} ---")


def run_aggr_pipeline():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        #  Get watermark
        cursor.execute("""
            SELECT last_processed_ts 
            FROM aggregation_watermarks 
            WHERE aggregation_interval = '1s'
        """)
        row = cursor.fetchone()

        if row and row[0]:
            last_proc_ts = row[0]
        else:
            cursor.execute("SELECT date_trunc('second', MIN(ts)) FROM raw_ticks")
            minrow = cursor.fetchone()
            last_proc_ts = minrow[0] if minrow and minrow[0] else datetime.now().replace(microsecond=0)

        print(f"Starting from: {last_proc_ts}")

        while True:
            time.sleep(0.5)

            curr_win_end = last_proc_ts + timedelta(seconds=1)

            # safety offset to avoid late data issues
            #safe_end = curr_win_end - SAFETY_OFFSET
            #if safe_end <= last_proc_ts:
                #continue
            safe_end = curr_win_end

            query = """
                SELECT 
                    date_trunc('second', ts) as bucket,
                    symbol,
                    (ARRAY_AGG(price ORDER BY ts ASC))[1],
                    MAX(price),
                    MIN(price),
                    (ARRAY_AGG(price ORDER BY ts DESC))[1],
                    SUM(volume)
                FROM raw_ticks
                WHERE ts > %s AND ts <= %s
                GROUP BY symbol, bucket
                ORDER BY bucket ASC
            """

            cursor.execute(query, (last_proc_ts, safe_end))
            rows = cursor.fetchall()

            if rows:
                ins_query = """
                    INSERT INTO ohlcv_1s 
                    (bucket_time, symbol, open_price, high_price, low_price, close_price, total_volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (bucket_time, symbol) DO NOTHING
                """

                for r in rows:
                    cursor.execute(ins_query, r)

            # minute rollup trigger (clean condition)
            if safe_end.second == 0:
                rollup_to_1m_table(cursor, safe_end)

            # ALWAYS move forward
            last_proc_ts = safe_end

            # CRITICAL: update watermark manually
            cursor.execute("""
                UPDATE aggregation_watermarks
                SET last_processed_ts = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE aggregation_interval = '1s'
            """, (last_proc_ts,))

            conn.commit()

            print(f"[Clock Time: {datetime.now().strftime('%H:%M:%S')}] Processed up to {last_proc_ts}")

    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    run_aggr_pipeline()