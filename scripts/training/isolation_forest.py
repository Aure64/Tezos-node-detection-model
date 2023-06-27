import sqlite3
import pandas as pd
from sklearn.ensemble import IsolationForest
import pickle

DB_NAME = 'data/tezos_metrics2.db'
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
conn = sqlite3.connect(DB_NAME)
print("Connected to the SQLite database")

# Load and concatenate all tables
df = pd.concat([pd.read_sql_query(f"SELECT * FROM {table}", conn) for table in TABLE_NAMES])
print("Loaded and concatenated all preprocessed tables")

# Train the model
print("Training the model...")
model = IsolationForest(contamination=0.01) # assuming 1% of the data points are anomalies
model.fit(df)

print("Model trained successfully.")

# Save the model
filename = 'models/isolation_forest_tezos_metrics.sav'
pickle.dump(model, open(filename, 'wb'))
print(f"Model saved to {filename}")

conn.close()
print("Closed the SQLite database connection")
