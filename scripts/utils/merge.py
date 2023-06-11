import sqlite3
import pandas as pd

def create_connection():
    conn = None
    try:
        conn = sqlite3.connect('tezos_metrics2.db')
        print(f'Successful connection with SQLite version {sqlite3.version}')
    except Error as e:
        print(f'Error occurred during SQLite connection: {e}')
    return conn

def merge_tables(conn):
    cursor = conn.cursor()

    # Create an empty DataFrame with unique timestamps
    query = "SELECT DISTINCT time FROM up_preprocessed"
    for table_name in ["octez_validator_block_worker_error_count_preprocessed", "ocaml_gc_allocated_bytes_preprocessed", "octez_p2p_connections_active_preprocessed", "process_cpu_seconds_total_preprocessed", "octez_store_last_merge_time_preprocessed", "octez_validator_chain_synchronisation_status_preprocessed", "octez_validator_chain_is_bootstrapped_preprocessed", "octez_validator_peer_system_error_preprocessed", "octez_validator_peer_unavailable_protocol_preprocessed", "octez_validator_peer_unknown_error_preprocessed", "octez_p2p_swap_fail_preprocessed", "octez_store_invalid_blocks_preprocessed", "process_start_time_seconds_preprocessed"]:
        query += f" UNION SELECT DISTINCT time FROM {table_name}"
    cursor.execute(query)
    rows = cursor.fetchall()

    # Get columns from cursor object
    columns = ['time']

    # Create dataframe from rows and columns
    merged_df = pd.DataFrame(rows, columns=columns)
    merged_df.set_index('time', inplace=True)

    # Join with each table
    for table_name in ["up_preprocessed", "octez_validator_block_worker_error_count_preprocessed", "ocaml_gc_allocated_bytes_preprocessed", "octez_p2p_connections_active_preprocessed", "process_cpu_seconds_total_preprocessed", "octez_store_last_merge_time_preprocessed", "octez_validator_chain_synchronisation_status_preprocessed", "octez_validator_chain_is_bootstrapped_preprocessed", "octez_validator_peer_system_error_preprocessed", "octez_validator_peer_unavailable_protocol_preprocessed", "octez_validator_peer_unknown_error_preprocessed", "octez_p2p_swap_fail_preprocessed", "octez_store_invalid_blocks_preprocessed", "process_start_time_seconds_preprocessed"]:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()

        # Get columns from cursor object
        columns = ['time', table_name]

        # Create dataframe from rows and columns
        df = pd.DataFrame(rows, columns=columns)
        df.set_index('time', inplace=True)

        # Merge
        merged_df = merged_df.join(df, how='left')

    return merged_df

# Create DB connection
conn = create_connection()

# Merge tables
merged_df = merge_tables(conn)

print(merged_df)
merged_df.to_sql('merged_tezos_metrics2', conn, if_exists='replace', index=True)

# Close DB connection
if conn:
    print('Closing DB connection')
    conn.close()
