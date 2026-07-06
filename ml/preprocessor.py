# PURPOSE — Scales the AQI data to a 0-1 range and creates input sequences for the LSTM model.
# WHY — LSTM neural networks work best when all input values are on the same small scale (0-1). Raw AQI values range from 35-400 — this large range confuses the network. Scaling fixes this. Sequences are needed because LSTM learns from patterns over time — "given the last 3 AQI readings, predict the next one."


# STEPS:
# step 1: scale_data() — scales features to 0-1 range
# step 2: create_sequences() — creates input/output pairs for LSTM


import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import logging

logger = logging.getLogger(__name__)



def scale_data(df: pd.DataFrame):
    features = ["avg_aqi", "avg_pm25", "avg_pm10"]
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(df[features])
    scaled_df = pd.DataFrame(scaled, columns=features)
    logger.info(f"Data scaled. Shape: {scaled_df.shape}")
    return scaled_df, scaler





def create_sequences(data: np.ndarray, sequence_length: int = 3):
    X, y = [], []
    for i in range(len(data) - sequence_length):
        X.append(data[i:i + sequence_length])
        y.append(data[i + sequence_length][0])
    X = np.array(X)
    y = np.array(y)
    logger.info(f"Sequences created. X shape: {X.shape}, y shape: {y.shape}")
    return X, y



# Why sequence_length=3?
# We only have ~20-60 records right now. With sequence_length=3 we get enough training samples. In production with months of data you'd use sequence_length=24 (24 hours).







# HOW preprocessor.py WORKS:
# scale_data() called with raw DataFrame from data_loader.py
# → MinMaxScaler fits on 5 features: avg_aqi, pm25, pm10, no2, so2
# → each column scaled independently: min=0, max=1
# → returns scaled_df + scaler object (needed to reverse predictions)
# → create_sequences() called with scaled numpy array
# → sequence_length=3 — LSTM sees 3 rows to predict the 4th
# → loop creates (X, y) pairs:
#     → X[0] = rows 0,1,2 (all 5 features) → y[0] = row 3 AQI
#     → X[1] = rows 1,2,3 (all 5 features) → y[1] = row 4 AQI
#     → continues until end of data
# → X shape: (n_samples, 3, 5) — samples, timesteps, features
# → y shape: (n_samples,) — one AQI value per sample
# → X and y passed to model.py for LSTM training