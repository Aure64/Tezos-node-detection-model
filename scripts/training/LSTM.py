import argparse
import sqlite3
import pandas as pd
import numpy as np
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.optimizers import Adam
import os
import tensorflow as tf
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # Suppress TensorFlow logging (1)

# Set up command line argument
parser = argparse.ArgumentParser(description='Train LSTM on the preprocessed SQLite database.')
parser.add_argument('--file', metavar='file', type=str, help='The path of the SQLite database file')
args = parser.parse_args()

# Get the database file path from the command line argument
db_file = args.file
print(f"Using database file: {db_file}")

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

def create_dataset(X, y, time_steps=1):
    Xs, ys = [], []
    for i in range(len(X) - time_steps):
        v = X.iloc[i:(i + time_steps)].values
        Xs.append(v)
        ys.append(y.iloc[i + time_steps])
    return np.array(Xs), np.array(ys)

# Connect to the SQLite database
print(f"Attempting to connect to database at {db_file}")
conn = sqlite3.connect(db_file)

# Load and concatenate all tables
df = pd.concat([pd.read_sql_query(f"SELECT * FROM {table}", conn) for table in TABLE_NAMES])

# Check the dataframe
print("\nFirst 5 rows of the DataFrame:")
print(df.head())

print("\nStructure of the DataFrame:")
print(df.info())

print("\nNumber of null or missing values in each column:")
print(df.isnull().sum())

print("\nNumber of unique values in each column:")
print(df.nunique())

print("\nStatistical summary of the DataFrame:")
print(df.describe())

print("Loaded and concatenated all preprocessed tables")

X, y = create_dataset(df['value'], df['value'], time_steps=10)
X = X.reshape(X.shape[0], X.shape[1], 1)  # LSTM expects 3D input (samples, time_steps, features)

# Train the model
print("Training the model...")
model = Sequential()
model.add(LSTM(64, input_shape=(X.shape[1], X.shape[2])))
model.add(Dense(1))
model.compile(loss='mean_squared_error', optimizer=Adam(lr=0.001))
model.fit(X, y, epochs=30, batch_size=32, validation_split=0.2, shuffle=False)

print("Model trained successfully.")

# Save the model
model.save('models/lstm_tezos_metrics.h5')
print("Model saved.")

conn.close()
print("Closed the SQLite database connection")
