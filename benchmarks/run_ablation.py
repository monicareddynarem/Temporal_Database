import sys
import os

# Add the parent directory to the path so we can import from data_sources and ingestion
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_srcs.mock_gen import generate_naive_batches
# from data_srcs.live_gen import generate_historic_batches
from data_srcs.nse_gen import generate_nse_batches
from ingestion.row_wise_ingester import ingest_row_wise
from ingestion.batch_list_ingester import ingest_batch_list
from ingestion.batch_numpy_ingester import ingest_batch_copy
from ingestion.compressed_ingester import ingest_compressed_arrays # Uncomment when ready

def print_menu():
    print("\n" + "="*50)
    print(" TICK DATA PIPELINE - ABLATION TEST FRAMEWORK")
    print("="*50)
    print("Please ensure you have applied the correct SQL schema")
    print("in your database BEFORE running the corresponding test.")
    print("-" * 50)
    print("1. TEST: Row-Wise INSERT (Requires baseline/indexed schema)")
    print("2. TEST: Batch List EXECUTE_VALUES (Requires baseline/indexed schema)")
    print("3. TEST: Batch COPY via NumPy/StringIO (Requires baseline/indexed schema)")
    print("4. TEST: Array-Bucketed Compression (Requires compressed schema)")
    print("0. Exit")
    print("="*50)

def main():
    while True:
        print_menu()
        choice = input("Select a test to run (0-4): ").strip()
        
        if choice == '0':
            print("Exiting framework.")
            break
            
        if choice not in ['1', '2', '3', '4']:
            print("Invalid selection.")
            continue
            
        try:
            print("\n--- TEST CONFIGURATION ---")
            duration = int(input("Duration (virtual minutes): "))
            speed = int(input("Speed multiplier (Virtual sec / Real sec): "))
            
            # Initialize the identical data generator for all tests
            #data_stream = generate_naive_batches(duration, speed)
            # data_stream = generate_historic_batches(duration)
            data_stream = generate_nse_batches()

            
            # Route to the selected ingester
            if choice == '1':
                print("\n>>> STARTING ROW-WISE ABLATION <<<")
                ingest_row_wise(data_stream)
            elif choice == '2':
                print("\n>>> STARTING BATCH LIST ABLATION <<<")
                ingest_batch_list(data_stream)
            elif choice == '3':
                print("\n>>> STARTING BATCH COPY ABLATION <<<")
                ingest_batch_copy(data_stream)
            elif choice == '4':
                print("\n>>> STARTING COMPRESSED ARRAY ABLATION <<<")
                ingest_compressed_arrays(data_stream) # Uncomment when you add this file
                # print("Make sure you have created ingestion/compressed_ingester.py!")
                
            print("\n=== TEST COMPLETE ===")
            
        except ValueError:
            print("Error: Please enter valid integers for duration and speed.")
        except KeyboardInterrupt:
            print("\nTest aborted by user.")

if __name__ == "__main__":
    main()