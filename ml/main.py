# PURPOSE — Ties all ML files together. Loads data, preprocesses, trains LSTM, logs to MLflow, prints predictions.



import logging
import numpy as np
from ml.data_loader import load_aqi_data
from ml.preprocessor import scale_data, create_sequences
from ml.model import train_model, predict
from ml.mlflow_tracker import setup_mlflow, log_experiment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def main():
    logger.info("Starting AQI ML pipeline...")
    setup_mlflow()
    df = load_aqi_data()
    scaled_df, scaler = scale_data(df)
    data = scaled_df.values
    X, y = create_sequences(data, sequence_length=3)
    if len(X) < 5:
        logger.warning(f"Only {len(X)} sequences — need more data. Run pipeline more times.")
        return
    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]
    logger.info(f"Train: {len(X_train)} sequences, Test: {len(X_test)} sequences")

    params = {
        "epochs": 50,
        "learning_rate": 0.001,
        "hidden_size": 64,
        "num_layers": 2,
        "sequence_length": 3,
        "input_features": 3
    }

    model, losses = train_model(
        X_train, y_train,
        epochs=params["epochs"],
        lr=params["learning_rate"]
    )

    predictions = predict(model, X_test, scaler)
    actual = scaler.inverse_transform(
        np.column_stack([y_test, np.zeros((len(y_test), 2))])
    )[:, 0]

    mae = log_experiment(model, losses, predictions, actual, params)

    print("\n=== AQI PREDICTIONS vs ACTUAL ===")
    for i, (pred, act) in enumerate(zip(predictions, actual)):
        print(f"Sample {i+1}: Predicted={pred:.1f}, Actual={act:.1f}, "
              f"Error={abs(pred-act):.1f}")
    print(f"\nMean Absolute Error: {mae:.2f} AQI units")
    logger.info("ML pipeline complete.")



if __name__ == "__main__":
    main()




# HOW ml/main.py WORKS:
# python -m ml.main called from project root
# → setup_mlflow() — configures mlruns/ folder + AQI_Forecasting experiment
# → load_aqi_data() — reads city_aqi_summary from gold/aqi_warehouse.duckdb
# → scale_data() — MinMaxScaler scales 5 features to 0-1 range
# → create_sequences() — creates (X, y) pairs with sequence_length=3
# → safety check: need at least 5 sequences to train meaningfully
# → 80/20 train/test split
# → params dict defined — epochs=50, lr=0.001, hidden=64, layers=2
# → train_model() — 50 epochs of LSTM training
#     → loss logged every 10 epochs — should decrease over time
# → predict() — generates predictions on test set in real AQI scale
# → actual values inverse_transformed for comparison
# → log_experiment() — logs params, metrics, model to MLflow
# → prints predicted vs actual for each test sample
# → MAE printed — average error in real AQI units
# → mlflow ui → http://localhost:5000 to visualize experiments