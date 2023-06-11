import numpy as np
import pandas as pd
import pickle
from sklearn.ensemble import IsolationForest

# Load the model from disk
filename = 'models/isolation_forest_tezos_metrics.sav'
model = pickle.load(open(filename, 'rb'))

# Suppose that new_df is your new data
new_df = pd.DataFrame()  # This should be replaced with your actual new data

# Predict anomalies in the new data
preds = model.predict(new_df)

# Print the prediction results
for i in range(len(preds)):
    if preds[i] == -1:
        print(f"Anomaly detected in data point at index {i}")

# If you want to add the prediction results to the new DataFrame:
new_df['anomaly'] = np.where(preds == -1, 'Yes', 'No')

# Save the new DataFrame with the prediction results
new_df.to_csv('new_data_with_predictions.csv', index=False)

