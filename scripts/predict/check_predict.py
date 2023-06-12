import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('data/tezos_metrics2.db')

# Load the predictions
df_predictions = pd.read_sql_query("SELECT * FROM predictions", conn)

# Filter the predictions for anomalies
df_anomalies = df_predictions[df_predictions['is_anomaly'] == -1]

# Print the predictions
print(df_anomalies)

# Close the connection
conn.close()
