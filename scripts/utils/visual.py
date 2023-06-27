import argparse
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# Set up command line argument
parser = argparse.ArgumentParser(description='Analyse the preprocessed SQLite database.')
parser.add_argument('--file', metavar='file', type=str, help='The path of the SQLite database file')
args = parser.parse_args()

# Get the database file path from the command line argument
db_file = args.file
print(f"Using database file: {db_file}")

TABLE_NAMES = [
    'octez_validator_block_worker_error_count_preprocessed', 
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
print(f"Attempting to connect to database at {db_file}")
conn = sqlite3.connect(db_file)

fig, axs = plt.subplots(len(TABLE_NAMES), figsize=(10, 6*len(TABLE_NAMES)))

for idx, table in enumerate(TABLE_NAMES):
    # Load a table
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    print(f"Loaded table {table}")

    # Display the number of unique values in the table
    unique_values = df['value'].nunique()
    print(f"Table {table} has {unique_values} unique values")

    # Create a line plot for the table
    axs[idx].plot(df['time'], df['value'])
    axs[idx].set_title(f'Time Series for {table}')
    axs[idx].set_xlabel('Time')
    axs[idx].set_ylabel('Value')

# Close the database connection
conn.close()
print("Closed the SQLite database connection")

plt.tight_layout()
plt.show()
