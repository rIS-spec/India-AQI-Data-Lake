# PURPOSE — Defines the LSTM neural network architecture using PyTorch and trains it on the AQI sequences.
# WHY — LSTM (Long Short-Term Memory) is designed specifically for sequential data — it remembers patterns over time. Regular neural networks forget previous inputs. LSTM has a "memory cell" that keeps important information across many timesteps — perfect for time-series AQI prediction.



# steps - 
# step 1: AQILSTMModel class — defines the network architecture
# step 2: train_model() — trains the model on sequences
# step 3: predict() — makes future AQI predictions



import torch
import torch.nn as nn
import numpy as np
import logging

logger = logging.getLogger(__name__)



class AQILSTMModel(nn.Module):
    def __init__(self, input_size=3, hidden_size=64, num_layers=2, output_size=1):
        super(AQILSTMModel, self).__init__()
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=0.2
        )
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        last_output = lstm_out[:, -1, :]
        prediction = self.fc(last_output)
        return prediction




def train_model(X_train, y_train, epochs=50, lr=0.001):
    model = AQILSTMModel()
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    X_tensor = torch.FloatTensor(X_train)
    y_tensor = torch.FloatTensor(y_train).unsqueeze(1)

    losses = []
    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()
        output = model(X_tensor)
        loss = criterion(output, y_tensor)
        loss.backward()
        optimizer.step()
        losses.append(loss.item())
        if (epoch + 1) % 10 == 0:
            logger.info(f"Epoch {epoch+1}/{epochs} — Loss: {loss.item():.4f}")

    return model, losses



def predict(model, X, scaler):
    model.eval()
    with torch.no_grad():
        X_tensor = torch.FloatTensor(X)
        predictions = model(X_tensor).numpy()
    dummy = np.zeros((len(predictions), 3))
    dummy[:, 0] = predictions[:, 0]
    real_predictions = scaler.inverse_transform(dummy)[:, 0]
    logger.info(f"Predictions generated: {len(real_predictions)} values")
    return real_predictions




# HOW model.py WORKS:
# AQILSTMModel defined — 2 LSTM layers + 1 Linear layer
#     → input_size=5: aqi, pm25, pm10, no2, so2 per timestep
#     → hidden_size=64: 64 memory units to learn patterns
#     → dropout=0.2: prevents overfitting on small dataset
#     → forward(): lstm_out -> last timestep -> linear -> 1 AQI value
# train_model() called with X_train, y_train sequences
#     → MSELoss measures prediction error
#     → Adam optimizer adjusts weights each epoch
#     → 50 epochs: model sees all data 50 times
#     → loss logged every 10 epochs — should decrease over time
#     → returns trained model + loss history for MLflow
# predict() called with trained model + scaler
#     → model.eval() disables dropout for clean predictions
#     → torch.no_grad() saves memory — no gradient tracking needed
#     → predictions in 0-1 scale → dummy array → inverse_transform
#     → returns real AQI values (35-400 range)