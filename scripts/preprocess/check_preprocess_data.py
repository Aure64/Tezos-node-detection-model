import sqlite3
import pandas as pd

DB_NAME = 'data/tezos_metrics.db'
OUTPUT_DB_NAME = 'data/tezos_metrics_preprocessed.db'

tables = [
    'octez_validator_block_worker_error_count_preprocessed', 
    'up_preprocessed', 
    'ocaml_gc_allocated_bytes_preprocessed', 
    'octez_p2p_connections_active_preprocessed',
    'process_cpu_seconds_total_preprocessed',
    'octez_store_last_merge_time_preprocessed',
    'octez_validator_chain_synchronisation_status_preprocessed',
    'octez_validator_chain_is_bootstrapped_preprocessed',
    'octez_validator_peer_system_error_preprocessed',
    'octez_validator_peer_unavailable_protocol_preprocessed',
    'octez_validator_peer_unknown_error_preprocessed',
    'octez_p2p_swap_fail_preprocessed',
    'octez_store_invalid_blocks_preprocessed',
    'process_start_time_seconds_preprocessed',
]

# Connect to the SQLite database
conn = sqlite3.connect(DB_NAME)

# Create a new SQLite database for storing the preprocessed data
output_conn = sqlite3.connect(OUTPUT_DB_NAME)

for table in tables:
    print(f"\nChecking table: {table}")
    
    # Load the data into a Pandas DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    print(f"Loaded {len(df)} rows from {table}")
    
    # Print summary statistics
    print("Summary statistics:")
    print(df.describe())
    
    # Check for missing values
    print("Number of missing values:")
    print(df.isnull().sum())
    
    # Store the preprocessed data into the output database
    df.to_sql(table, output_conn, if_exists='replace', index=False)
    print(f"Preprocessed data saved for table: {table}")

# Close the database connections
conn.close()
output_conn.close()
