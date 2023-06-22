import sqlite3
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import argparse

parser = argparse.ArgumentParser(description='Preprocess the SQLite database for LSTM.')
parser.add_argument('--file', metavar='file', type=str, help='The path of the SQLite database file')
args = parser.parse_args()

db_file = args.file
print(f"Using database file: {db_file}")



DB_NAME = 'data/tezos_metrics2.db'
TABLE_NAMES = [
    'octez_validator_block_worker_error_count', 
    'ocaml_gc_allocated_bytes', 
    'octez_p2p_connections_active',
    'process_cpu_seconds_total',
    'octez_store_last_merge_time',
    'octez_validator_chain_synchronisation_status',
    'octez_validator_chain_is_bootstrapped',
    'octez_validator_peer_system_error',
    'octez_validator_peer_unavailable_protocol',
    'octez_validator_peer_unknown_error',
    'octez_p2p_swap_fail',
    'octez_store_invalid_blocks',
    'process_start_time_seconds',
]

print(f"Attempting to connect to database at {db_file}")

# Connect to the SQLite database
conn = sqlite3.connect(db_file)
print(f"\nConnected to the SQLite database: {db_file}")

for table in TABLE_NAMES:
    print(f"\nProcessing table: {table}")

    # Check if preprocessed table already exists
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}_preprocessed'")
    if cursor.fetchone():
        print(f"Table {table}_preprocessed already exists, skipping preprocessing.")
        continue

    # Load the data into a Pandas DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    print(f"Loaded {len(df)} rows from {table}")

    # Convert time to datetime and then to total seconds
    df['time'] = pd.to_datetime(df['time'])
    start_time = df['time'].min()
    df['time'] = (df['time'] - start_time).dt.total_seconds()
    print("Converted time to total seconds")

    # Fill missing values with the mean of the column
    df = df.fillna(df.mean(numeric_only=True))
    print("Filled missing values with the mean")

    # Normalizing the data with MinMaxScaler
    min_max_scaler = MinMaxScaler()
    x = df[['value']].values.astype(float)
    x_scaled = min_max_scaler.fit_transform(x)
    df_normalized = pd.DataFrame(x_scaled, columns=['value'], index=df.index)
    df['value'] = df_normalized['value']
    print("Normalized data")

    # Save the preprocessed data back to the SQLite database
    df.to_sql(table + "_preprocessed", conn, if_exists='replace', index=False)
    print("Saved preprocessed data back to the SQLite database")

conn.close()
print("Done with preprocessing!")
