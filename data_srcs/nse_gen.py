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
    
    print(f"\n CONNECTING TO LIVE NSE API (Polling every {poll_interval_seconds}s)...")
    print("  Warning: Do not lower the poll interval below 2s or the NSE will block your IP.")
    print("-" * 60)
    
    while True:
        gen_start = time.time()
        
        
        current_time = datetime.now()
        batch_list = []
        
        for sym in symbols_list:
            try:
                
                meta = nse_eq(sym)                
                price = float(meta['priceInfo']['lastPrice'])
                try:
                    volume = int(meta['marketDeptOrderBook']['tradeInfo']['totalTradedVolume'])
                except (KeyError, TypeError):
                    volume = 100

                batch_list.append((sym, price, volume, current_time))
                
                time.sleep(0.1) 
                
            except Exception as e:
                # If a symbol fails or the network blips, skip it silently to keep the pipeline alive
                pass 

        gen_time = time.time() - gen_start
        total_ticks = len(batch_list)
        
        
        if total_ticks > 0:
            yield batch_list, current_time, gen_time, total_ticks
        
        elapsed = time.time() - gen_start
        if elapsed < poll_interval_seconds:
            time.sleep(poll_interval_seconds - elapsed)