import argparse
import sqlite3
import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model

# Set up command line argument
parser = argparse.ArgumentParser(description='Predict anomalies in the SQLite database using trained LSTM model.')
parser.add_argument('--db_file', metavar='db_file', type=str, help='The path of the SQLite database file')
parser.add_argument('--model_file', metavar='model_file', type=str, help='The path of the LSTM model file (.h5)')
args = parser.parse_args()

# Get the database file path and model file path from the command line argument
db_file = args.db_file
model_file = args.model_file
print(f"Using database file: {db_file}")
print(f"Using model file: {model_file}")

DB_NAME = 'data/tezos_metrics3.db'
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

def create_dataset(X, time_steps=1):
    Xs = []
    for i in range(len(X) - time_steps):
        v = X.iloc[i:(i + time_steps)].values
        Xs.append(v)
    return np.array(Xs)

# Connect to the SQLite database
print(f"Attempting to connect to database at {db_file}")
conn = sqlite3.connect(db_file)

# Load and concatenate all tables
df = pd.concat([pd.read_sql_query(f"SELECT * FROM {table}", conn) for table in TABLE_NAMES])

# Load the trained model
print("Loading the trained model...")
model = load_model(model_file)
print("Model loaded successfully.")

X = create_dataset(df['value'], time_steps=10)
X = X.reshape(X.shape[0], X.shape[1], 1)  # LSTM expects 3D input (samples, time_steps, features)

# Make predictions
print("Making predictions...")
predictions = model.predict(X)
df = df.iloc[10:]  # remove the first 10 rows because they do not have predictions
df['prediction'] = predictions
print("Predictions made successfully.")

# Save predictions to the SQLite database
print("Saving predictions back to the SQLite database...")
df.to_sql("predictions", conn, if_exists='replace', index=False)
print("Predictions saved successfully.")

conn.close()
print("Closed the SQLite database connection")
