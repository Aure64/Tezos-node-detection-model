import sqlite3
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

DB_NAME = 'data/tezos_metrics2.db'

tables = [
    'octez_validator_block_worker_error_count', 
    'up', 
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

# Connect to the SQLite database
print("Connecting to SQLite database...")
conn = sqlite3.connect(DB_NAME)

for table in tables:
    print(f"\nProcessing table: {table}")
    
    # Load the data into a Pandas DataFrame
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    print(f"Loaded {len(df)} rows from {table}")

    # Convert time to datetime and then to total seconds
    df['time'] = pd.to_datetime(df['time'])
    start_time = df['time'].min()
    df['time'] = (df['time'] - start_time).dt.total_seconds()

    # Split the data into training and testing sets
    df_train, df_test = train_test_split(df, test_size=0.2, random_state=42)

    # Fill missing values with the mean of the column in the training set and apply to both sets
    print("Filling missing values with the mean...")
    df_train = df_train.fillna(df_train.mean(numeric_only=True))
    df_test = df_test.fillna(df_train.mean(numeric_only=True))

    # Normalizing the data with MinMaxScaler
    print("Normalizing data...")
    min_max_scaler = MinMaxScaler()
    x_train = df_train[['value']].values.astype(float)
    x_test = df_test[['value']].values.astype(float)
    x_train_scaled = min_max_scaler.fit_transform(x_train)
    x_test_scaled = min_max_scaler.transform(x_test)
    df_train_normalized = pd.DataFrame(x_train_scaled, columns=['value'], index=df_train.index)
    df_test_normalized = pd.DataFrame(x_test_scaled, columns=['value'], index=df_test.index)
    df_train['value'] = df_train_normalized['value']
    df_test['value'] = df_test_normalized['value']
    
    # Save the preprocessed data back to the SQLite database
    print("Saving preprocessed data back to the SQLite database...")
    df_train.to_sql(table + "_preprocessed_train", conn, if_exists='replace', index=False)
    df_test.to_sql(table + "_preprocessed_test", conn, if_exists='replace', index=False)
    
print("\nAll done!")
conn.close()
