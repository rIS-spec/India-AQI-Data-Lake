# PURPOSE — Logs every ML experiment to MLflow — model parameters, training loss, final accuracy, and saves the trained model so you can compare different runs.
# WHY — Without MLflow, if you train 5 models with different parameters you have no record of which one performed best. MLflow tracks every experiment automatically — you can go back and reproduce any result.



# steps - 
# step 1: setup_mlflow() — configures experiment name
# step 2: log_experiment() — logs parameters, metrics, and model


import mlflow
import mlflow.pytorch
import logging

logger = logging.getLogger(__name__)



def setup_mlflow(experiment_name="AQI_Forecasting"):
    mlflow.set_tracking_uri("mlruns")
    mlflow.set_experiment(experiment_name)
    logger.info(f"MLflow experiment set: {experiment_name}")



def log_experiment(model, losses, predictions, actual, params):
    with mlflow.start_run():
        mlflow.log_params(params)
        mlflow.log_metric("final_loss", losses[-1])
        mlflow.log_metric("min_loss", min(losses))
        for i, loss in enumerate(losses):
            mlflow.log_metric("training_loss", loss, step=i)
        mae = float(sum(abs(p - a) for p, a in
                    zip(predictions, actual)) / len(predictions))
        mlflow.log_metric("mae", mae)
        mlflow.pytorch.log_model(model, "aqi_lstm_model")
        logger.info(f"MLflow run logged. MAE: {mae:.2f}")
        return mae




# HOW mlflow_tracker.py WORKS:
# setup_mlflow() called once at start of main.py
# → mlflow.set_tracking_uri("mlruns") — saves to local mlruns/ folder
# → mlflow.set_experiment("AQI_Forecasting") — groups all runs together
# → log_experiment() called after training completes
# → mlflow.start_run() — opens new run with auto-generated ID
# → mlflow.log_params() — saves hyperparameters:
#     → epochs, learning_rate, hidden_size, num_layers, sequence_length
# → mlflow.log_metric("final_loss") — last epoch loss
# → mlflow.log_metric("training_loss", step=i) — loss per epoch
#     → creates loss curve visible in MLflow UI
# → MAE calculated — average |predicted - actual| in real AQI units
# → mlflow.log_metric("mae") — model accuracy in real AQI units
# → mlflow.pytorch.log_model() — saves entire model to mlruns/
#     → model can be loaded later: mlflow.pytorch.load_model(run_id)
# → run closed automatically when 'with' block exits
# → MLflow UI: mlflow ui → http://localhost:5000 to compare all runs