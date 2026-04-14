import time
from datetime import datetime
try:
    from nsepython import nse_eq
except ImportError:
    print("Error: nsepython not found. Run 'pip install nsepython'")
    exit(1)

# A sample of highly liquid Nifty 50 stocks
symbols_list = ['RELIANCE', 'TCS', 'INFY', 'HDFCBANK', 'ICICIBANK', 'SBIN', 'BHARTIARTL', 'ITC', 'LT', 'BAJFINANCE']

def generate_nse_batches(poll_interval_seconds=3):
    """
    Fetches live stock data from NSE via nsepython.
    Yields batches perfectly formatted for the batch_copy ingester.
    """
    print(f"\n🌐 CONNECTING TO LIVE NSE API (Polling every {poll_interval_seconds}s)...")
    print("⚠️  Warning: Do not lower the poll interval below 2s or the NSE will block your IP.")
    print("-" * 60)
    
    while True:
        gen_start = time.time()
        
        # We grab the current actual time to serve as our "virtual" time
        current_time = datetime.now()
        batch_list = []
        
        for sym in symbols_list:
            try:
                # Fetch the live JSON payload from the NSE website
                meta = nse_eq(sym)
                
                # Extract Last Traded Price (LTP)
                price = float(meta['priceInfo']['lastPrice'])
                
                # Extract Volume. NSE JSON structures can be fragile, so we use a fallback
                try:
                    volume = int(meta['marketDeptOrderBook']['tradeInfo']['totalTradedVolume'])
                except (KeyError, TypeError):
                    volume = 100 # Fallback default if NSE hides volume data temporarily

                # Append to the batch in the exact order your ingester expects
                batch_list.append((sym, price, volume, current_time))
                
                # A micro-sleep prevents the NSE firewall from identifying us as a bot
                time.sleep(0.1) 
                
            except Exception as e:
                # If a symbol fails or the network blips, skip it silently to keep the pipeline alive
                pass 

        gen_time = time.time() - gen_start
        total_ticks = len(batch_list)
        
        # Yield the exact 4 variables your ingest_batch_copy script is unpacking
        if total_ticks > 0:
            yield batch_list, current_time, gen_time, total_ticks
        
        # Pacing: Ensure we wait the full poll_interval before hitting the NSE servers again
        elapsed = time.time() - gen_start
        if elapsed < poll_interval_seconds:
            time.sleep(poll_interval_seconds - elapsed)