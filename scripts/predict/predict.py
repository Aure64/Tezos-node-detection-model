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

MODEL_PATH = 'models/isolation_forest_tezos_metrics.sav'

# Load the trained model
print("Loading the trained model...")
model = pickle.load(open(MODEL_PATH, 'rb'))
print("Model loaded successfully.")

# Connect to the SQLite database
print("Connecting to SQLite database...")
conn = sqlite3.connect(DB_NAME)

# Load and concatenate all tables
print("Loading and concatenating all preprocessed tables...")
df = pd.concat([pd.read_sql_query(f"SELECT * FROM {table}", conn) for table in TABLE_NAMES])

# Make predictions
print("Making predictions...")
predictions = model.predict(df)
df['is_anomaly'] = predictions
print("Predictions made successfully.")

# Save predictions to the SQLite database
print("Saving predictions back to the SQLite database...")
df.to_sql("predictions", conn, if_exists='replace', index=False)
print("Predictions saved successfully.")

conn.close()
