from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockTradesRequest
from datetime import datetime, timedelta, timezone
import time
import os
from utils.connection import get_db_connection

API_KEY = os.getenv("APCA_API_KEY_ID")
SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")

client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
TIME_OFFSET = timedelta(days=365 * 2)
def fetch_chunk(start_time, end_time):
    request = StockTradesRequest(
        symbol_or_symbols=['GOOGL','META','TSLA','NVDA','AMZN','NFLX','MSFT','AAPL','TSMC','INTC'],  # multiple symbols
        start=start_time,
        end=end_time
    )

    trades = client.get_stock_trades(request)

    batch = []

    for symbol, trade_list in trades.data.items():
        for t in trade_list:
            batch.append((
                symbol,
                float(t.price),
                int(t.size),
                t.timestamp.replace(tzinfo=None)
            ))

    batch.sort(key=lambda x: x[3])
    return batch


def generate_historic_batches(duration):
    window_size = timedelta(minutes=10)
    insflag = True
    while True:
        now = datetime.now(timezone.utc)
        
        hist_start = (now - TIME_OFFSET).replace(tzinfo=None, microsecond=0)
        hist_end = hist_start + window_size

        if insflag:
            conn = get_db_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO aggregation_watermarks (aggregation_interval, last_processed_ts)
                        VALUES (%s, %s)
                        ON CONFLICT (aggregation_interval) 
                        DO NOTHING
                    """, ('1s', hist_start.replace(microsecond=0)))
                conn.commit()
            finally:
                conn.close()

            insflag = False

        data = fetch_chunk(hist_start, hist_end)

        # emit per second (like real stream)
        batch = []
        current_second = None

        for row in data:
            ts = row[3].replace(microsecond=0)

            if current_second is None:
                current_second = ts

            if ts == current_second:
                batch.append(row)
            else:
                gen_time = 0.0  # since API fetch already happened earlier
                total_ticks = sum(b[2] for b in batch)
                current_v_time = batch[0][3] # not sure what to be done

                yield batch, current_v_time, gen_time, total_ticks
                time.sleep(1)

                batch = [row]
                current_second = ts

        if batch:
            gen_time = 0.0
            total_ticks = len(batch)
            current_v_time = batch[0][3]
            yield batch, current_v_time, gen_time, total_ticks

        
