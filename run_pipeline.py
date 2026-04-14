import sys
import os


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_srcs.mock_gen import generate_naive_batches
from data_srcs.live_gen import generate_historic_batches
from ingestion.row_wise_ingester import ingest_row_wise
from ingestion.batch_list_ingester import ingest_batch_list
from ingestion.batch_numpy_ingester import ingest_batch_copy
from ingestion.compressed_ingester import ingest_compressed_arrays 
from utils.connection import get_db_connection 

def apply_database_schema(choice):
    
    sql_filename = 'compressed_indexed.sql' if choice == '4' else 'indexed.sql'
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_filepath = os.path.join(project_root, 'schemas', sql_filename)
    
    print(f"\n Architecture switch detected or DB empty!")
    print(f" Rebuilding tables using {sql_filename}...")
    
    conn = get_db_connection()
    try:
        with open(sql_filepath, 'r') as file:
            sql_script = file.read()
            
        with conn.cursor() as cursor:
            cursor.execute(sql_script)
        conn.commit()
        print(f" Database rebuilt flawlessly with {sql_filename}!")
        
    except FileNotFoundError:
        print(f"\n[!] FATAL ERROR: Could not find '{sql_filename}'.")
        print("Please ensure your .sql files are in the same directory as run_ablation.py!")
        sys.exit(1)
    except Exception as e:
        conn.rollback()
        print(f"\n[!] SQL Execution Error: {e}")
        sys.exit(1)
    finally:
        conn.close()

def check_and_prepare_schema(choice):
    
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'raw_ticks_bucketed');")
            db_has_compressed = cursor.fetchone()[0]
            
            cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'raw_ticks');")
            db_has_standard = cursor.fetchone()[0]
            
        target_is_compressed = (choice == '4')

        if target_is_compressed:
            if db_has_compressed:
                print("\n[INFO] Schema matches (Compressed). Preparing to resume...")
                return True 
            else:
                apply_database_schema(choice)
                return False 
        else:
            if db_has_standard:
                print("\n[INFO] Schema matches (Standard). Preparing to resume...")
                return True 
            else:
                apply_database_schema(choice)
                return False 
    finally:
        conn.close()

def print_menu():
    print("\n" + "="*50)
    print(" TICK DATA PIPELINE - ABLATION TEST FRAMEWORK")
    print("="*50)
    print("1. TEST: Row-Wise INSERT (Requires standard schema)")
    print("2. TEST: Batch List EXECUTE_VALUES (Requires standard schema)")
    print("3. TEST: Batch COPY via NumPy/StringIO (Requires standard schema)")
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
            
            
            check_and_prepare_schema(choice)
            
            data_stream = generate_historic_batches(duration)
            
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
                ingest_compressed_arrays(data_stream) 
                
            print("\n=== TEST COMPLETE ===")
            
        except ValueError:
            print("Error: Please enter valid integers for duration and speed.")
        except KeyboardInterrupt:
            print("\nTest aborted by user.")

if __name__ == "__main__":
    main()