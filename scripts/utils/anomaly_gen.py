import sqlite3
import pandas as pd
import numpy as np
import random

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

# Create a new database for anomaly testing
conn_anomaly = sqlite3.connect('data/tezos_metrics_anomaly.db')

# Connect to the original database
conn_original = sqlite3.connect(DB_NAME)

for table in TABLE_NAMES:
    print(f"\nProcessing table: {table}")
    
    # Load the data into a Pandas DataFrame from the original DB
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn_original)

    # Identify rows to which we will add anomalies (randomly select 1% of the rows)
    anomaly_rows = df.sample(frac=0.01).index

    # For the selected rows, add a large random value to simulate anomaly
    for i in anomaly_rows:
        df.loc[i, 'value'] = df.loc[i, 'value'] + random.uniform(10, 20) 

    # Save the data with anomalies to the new SQLite database
    df.to_sql(table, conn_anomaly, if_exists='replace', index=False)

conn_anomaly.close()
conn_original.close()
print("\nAll done!")
