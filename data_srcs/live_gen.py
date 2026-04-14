from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockTradesRequest
from datetime import datetime, timedelta, timezone
import time
from dotenv import load_dotenv
import os
from utils.connection import get_db_connection

load_dotenv()
API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

def fetch_chunk(start_time, end_time):
    request = StockTradesRequest(
        symbol_or_symbols=['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC'],
        start=start_time,
        end=end_time
    )
    trades = client.get_stock_trades(request)
    batch = []
    for symbol, trade_list in trades.data.items():
        for t in trade_list:
            batch.append((symbol, float(t.price), int(t.size), t.timestamp.replace(tzinfo=None)))
    batch.sort(key=lambda x: x[3])
    return batch

def generate_historic_batches(duration):
    window_size = timedelta(minutes=1)
    insflag = True
    
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT last_processed_ts FROM aggregation_watermarks WHERE aggregation_interval = '1s'")
            row = cursor.fetchone()
            if row:
                current_start = row[0].replace(tzinfo=None)
                print(f"\n[RESUME] Found existing DB watermark. Resuming Alpaca download from {current_start}")
            else:
                current_start = datetime(2024, 4, 11, 14, 0, 0)
                print(f"\n[START] Clean database detected. Starting fresh from {current_start}")
    except Exception:
        current_start = datetime(2024, 4, 11, 14, 0, 0)
    finally:
        if 'conn' in locals() and conn:
            conn.close()

    while True:
        hist_start = current_start
        hist_end = hist_start + window_size

        if insflag:
            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO aggregation_watermarks (aggregation_interval, last_processed_ts)
                        VALUES (%s, %s)
                        ON CONFLICT (aggregation_interval) 
                        DO UPDATE SET last_processed_ts = EXCLUDED.last_processed_ts
                    """, ('1s', hist_start.replace(microsecond=0)))
                conn.commit()
            finally:
                conn.close()
            insflag = False

        print(f"\n[DOWNLOAD] Fetching Alpaca tick data from {hist_start} to {hist_end}...")
        data = fetch_chunk(hist_start, hist_end)
        print(f"[DOWNLOAD] Success! Downloaded {len(data)} total trades. Passing to ingester...")

        if not data:
            print("[WARNING] No trades found (Market closed?). Waiting 5 seconds before trying again...")
            time.sleep(5)
            current_start = hist_end 
            continue

        batch = []
        current_second = None

        for row in data:
            ts = row[3].replace(microsecond=0)
            if current_second is None:
                current_second = ts

            if ts == current_second:
                batch.append(row)
            else:
                gen_time = 0.0 
                total_ticks = sum(b[2] for b in batch)
                current_v_time = batch[0][3]
                yield batch, current_v_time, gen_time, total_ticks
                time.sleep(1)
                batch = [row]
                current_second = ts

        if batch:
            gen_time = 0.0
            total_ticks = len(batch)
            current_v_time = batch[0][3]
            yield batch, current_v_time, gen_time, total_ticks

        current_start = hist_end