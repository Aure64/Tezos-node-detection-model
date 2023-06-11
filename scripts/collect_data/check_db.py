import sqlite3
import pandas as pd

# Crée une connexion à la base de données
conn = sqlite3.connect('data/tezos_metrics.db')

# Liste des noms de table que vous voulez vérifier
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

# Pour chaque table, exécute une requête SELECT et affiche les résultats
for table in tables:
    query = f"SELECT * FROM {table}"
    df = pd.read_sql_query(query, conn)
    print(f"Table: {table}")
    print(df)

# Ferme la connexion à la base de données
conn.close()
