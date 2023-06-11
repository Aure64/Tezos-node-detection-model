import os
import sqlite3
import time
import requests
import pandas as pd

# SQLite DB Name
DB_NAME = 'tezos_metrics.db'

DB_FILE_PATH = "data/tezos_metrics.db" 

MAX_DB_SIZE = 10 * 1024 * 1024 * 1024 # 10 GB

# Prometheus query URL
URL = 'http://baker.paris:9090/api/v1/query'

# List of queries you are interested in
queries = [
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

def run_query(query):
    print(f"Running query: {query}")
    response = requests.get(URL, params={'query': query})
    data = response.json()
    
    if data['status'] != 'success':
        print(f'Error in query execution: {data["error"] if "error" in data else "Unknown error"}')
        return None
        
    value = float(data['data']['result'][0]['value'][1])
    print(f"Query {query} executed successfully. Value: {value}")
    return value

def write_to_db(conn, df, table_name):
    print(f"Writing data to table {table_name}")
    df.to_sql(table_name, conn, if_exists='append', index=False)
    print(f"Data successfully written to table {table_name}")

def connect_to_db(db_name):
    print("Connecting to SQLite database...")
    conn = sqlite3.connect(db_name)
    print(f"Successful connection with SQLite database: {db_name}")
    return conn

def create_table(conn, table_name):
    print(f"Creating table if not exists: {table_name}")
    cursor = conn.cursor()
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (time datetime, value real)")
    conn.commit()
    print(f"Table {table_name} created successfully")

def check_db_size():
    db_size = os.path.getsize(DB_FILE_PATH)
    print(f"Checking database size. Current size: {db_size / (1024 * 1024 * 1024)} GB")
    if db_size >= MAX_DB_SIZE:
        print("Database size limit reached. Stopping data collection.")
        return False
    else:
        return True

while True:
    if not check_db_size():
        break

    conn = connect_to_db(DB_NAME)
    
    for query in queries:
        create_table(conn, query)
        
        value = run_query(query)
        if value is not None:
            df = pd.DataFrame({'time': [pd.Timestamp.now()], 'value': [value]})
            write_to_db(conn, df, query)
            
    print("Closing database connection")
    conn.close()
    
    # Wait for 10 minutes before running the queries again
    print("Waiting for 5 minutes before the next cycle...")
    time.sleep(300)
