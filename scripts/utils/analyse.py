import sqlite3
import pandas as pd

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

# Connect to the SQLite database
conn = sqlite3.connect(DB_NAME)

# Load the predictions
df_predictions = pd.read_sql_query("SELECT * FROM predictions", conn)

# Filter out the anomalies
df_anomalies = df_predictions[df_predictions['is_anomaly'] == -1]

# Create an empty DataFrame to store the original metrics values at the anomaly times
df_anomaly_metrics = pd.DataFrame()

for table in TABLE_NAMES:
    # Load the original data
    df_original = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    
    # Convert 'time' in df_original to datetime and calculate total seconds
    df_original['time'] = pd.to_datetime(df_original['time'])
    start_time = df_original['time'].min()
    df_original['time'] = (df_original['time'] - start_time).dt.total_seconds()

    # Merge the anomalies DataFrame with the original data for this table on 'time'
    df_merged = pd.merge(df_anomalies, df_original, on='time', how='inner', suffixes=('_pred', '_orig'))

    # Append the result to the anomaly metrics DataFrame
    df_anomaly_metrics = df_anomaly_metrics.append(df_merged, ignore_index=True)

# Save the anomaly metrics to a CSV file
df_anomaly_metrics.to_csv('anomaly_metrics.csv', index=False)

print("Anomaly metrics saved to anomaly_metrics.csv")

# Close the connection
conn.close()
