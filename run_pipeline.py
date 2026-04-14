import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from data_srcs.mock_gen import generate_naive_batches
    from data_srcs.nse_gen import generate_nse_batches
    from data_srcs.live_gen import generate_historic_batches
except ImportError as e:
    print(f" Import Error. Make sure your generator files exist: {e}")
    sys.exit(1)

from ingestion.batch_numpy_ingester import ingest_batch_copy 

def print_menu():
    print("\n" + "="*50)
    print("  TICK DATA INGESTION PIPELINE ")
    print("="*50)
    print("Select your Data Source:")
    print("1.  Live NSE Data (nsepython, polls every 3s)")
    print("2.  Historical Replay (yfinance, 1-min fractal ticks)")
    print("3.  Mock Data (High-speed simulated stress test)")
    print("0. Exit")
    print("="*50)

def main():
    while True:
        print_menu()
        choice = input("Enter choice (0-3): ").strip()

        if choice == '0':
            print("Exiting pipeline.")
            break
            
        try:
            if choice == '1':
                print("\n>>> STARTING LIVE NSE FEED <<<")
                data_stream = generate_nse_batches(poll_interval_seconds=3)
                ingest_batch_copy(data_stream)

            elif choice == '2':
                print("\n>>> STARTING HISTORICAL REPLAY <<<")
                start_date = input("Enter Start Date (YYYY-MM-DD): ").strip()
                end_date = input("Enter End Date (YYYY-MM-DD): ").strip()
                
                data_stream = generate_historic_batches(start_date, end_date)
                
                if data_stream:
                    ingest_batch_copy(data_stream)

            elif choice == '3':
                print("\n>>> STARTING MOCK STRESS TEST <<<")
                duration = int(input("Duration (virtual minutes): "))
                speed = int(input("Speed multiplier (Virtual sec / Real sec): "))
                
                data_stream = generate_naive_batches(duration, speed)
                ingest_batch_copy(data_stream)

            else:
                print("Invalid selection. Try again.")

        except KeyboardInterrupt:
            print("\n Pipeline safely stopped by user.")
        except Exception as e:
            print(f"\n Pipeline Error: {e}")

if __name__ == "__main__":
    main()