import sys
import os

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from data_srcs.live_gen import generate_historic_batches
from ingestion.batch_numpy_ingester import ingest_batch_copy
from ingestion.compressed_ingester import ingest_compressed_arrays 
from utils.connection import get_db_connection

def apply_database_schema(schema_filename):
    """wipes all tables in the public schema, then rebuilds."""
    project_root = os.path.abspath(os.path.dirname(__file__))
    sql_filepath = os.path.join(project_root, 'schemas', schema_filename)
    
    print(f"\nDropping all tables and restarting...")
    
    conn = get_db_connection()
    try:
        with open(sql_filepath, 'r') as file:
            sql_script = file.read()
            
        with conn.cursor() as cursor:
            cursor.execute("""
                DO $$ 
                DECLARE 
                    r RECORD; 
                BEGIN 
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') 
                    LOOP 
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; 
                    END LOOP; 
                END $$;
            """)
            
            cursor.execute(sql_script)
            
        conn.commit()
        print(f"Database rebuilt cleanly using {schema_filename}!")
        
    except FileNotFoundError:
        print(f"\n[!] FATAL ERROR: Could not find '{schema_filename}'.")
        print("Please ensure your .sql files are inside the 'schemas' directory!")
        sys.exit(1)
    except Exception as e:
        conn.rollback()
        print(f"\n[!] SQL Execution Error: {e}")
        sys.exit(1)
    finally:
        conn.close()

def print_menu():
    print("\n" + "="*50)
    print(" AUTOMATED ABLATION TEST RUNNER")
    print("="*50)
    print("1. Baseline Storage (No Indexes)")
    print("2. Indexed Storage (B-Tree Indexes)")
    print("3. Compressed Array Storage (LZ4 Arrays)")
    print("0. Exit")
    print("="*50)

def main():
    while True:
        print_menu()
        choice = input("Select Architecture to Benchmark (0-3): ").strip()
        
        if choice == '0':
            print("Exiting framework.")
            break
            
        if choice not in ['1', '2', '3']:
            print("Invalid selection.")
            continue
            
        try:
            print("\nTEST CONFIGURATION:")
            duration = int(input("Duration (virtual minutes): "))
            speed = int(input("Speed multiplier (e.g., 10x): "))
            
            if choice == '1':
                schema_filename = 'baseline.sql'
                ingester = ingest_batch_copy
                test_name = "BASELINE"
            elif choice == '2':
                schema_filename = 'indexed.sql'
                ingester = ingest_batch_copy
                test_name = "INDEXED"
            elif choice == '3':
                schema_filename = 'compressed_indexed.sql'
                ingester = ingest_compressed_arrays
                test_name = "COMPRESSED ARRAY"

            apply_database_schema(schema_filename)
            
            data_stream = generate_historic_batches(duration, speed)
            
            print(f"\n>>> PUSHING DATA FOR {test_name} TEST (Speed: {speed}x) <<<")
            ingester(data_stream) 
                
            print("\n=== INGESTION COMPLETE ===")
            print("You can now check your live benchmarks in Streamlit.")
            
        except ValueError:
            print("Error: Please enter valid integers for duration and speed.")
        except KeyboardInterrupt:
            print("\nTest aborted by user.")
        

if __name__ == "__main__":
    main()