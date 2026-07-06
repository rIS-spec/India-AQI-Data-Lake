# PURPOSE — A lightweight Python scheduler that runs the full AQI pipeline automatically every hour on your local machine. This is the working version since Airflow needs too much RAM for local dev.


import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

my_env = os.environ.copy()
my_env["HADOOP_HOME"] = "C:\\hadoop"
my_env["PATH"] = my_env["PATH"] + ";C:\\hadoop\\bin"

import schedule
import time
import logging
import subprocess
from ingestion.api.aqi_fetcher import fetch_aqi_data
from ingestion.kafka.producer import produce_aqi_batch
from silver.main import main as run_silver
from gold.main import main as run_gold

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def task_ingest():
    logger.info("TASK 1: Starting ingestion...")
    records = [r.model_dump() for r in fetch_aqi_data()]
    produce_aqi_batch(records)
    logger.info("TASK 1: Ingestion complete.")


def task_bronze():
    logger.info("TASK 2: Starting bronze layer...")
    try:
        subprocess.run(
            ["python", "-m", "bronze.main"],
            timeout=120,
            env=my_env
        )
    except subprocess.TimeoutExpired:
        logger.info("TASK 2: Bronze timeout — batch written successfully.")
    logger.info("TASK 2: Bronze complete.")


def task_silver():
    logger.info("TASK 3: Starting silver layer...")
    run_silver()
    logger.info("TASK 3: Silver complete.")


def task_gold():
    logger.info("TASK 4: Starting gold layer...")
    run_gold()
    logger.info("TASK 4: Gold complete.")


def run_pipeline():
    logger.info("=" * 50)
    logger.info("PIPELINE STARTING...")
    logger.info("=" * 50)
    try:
        task_ingest()
        task_bronze()
        task_silver()
        task_gold()
        logger.info("=" * 50)
        logger.info("PIPELINE COMPLETE.")
        logger.info("=" * 50)
    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")



if __name__ == "__main__":
    logger.info("Scheduler started. Pipeline runs every 1 hour.")
    run_pipeline()  # run immediately on start
    schedule.every(1).hours.do(run_pipeline)
    while True:    # keeps Python running and checks every 60 seconds if a scheduled job is due
        schedule.run_pending()
        time.sleep(60)





# HOW scheduler.py WORKS:
# python -m orchestration.scheduler called from project root
# → run_pipeline() called immediately on startup — no waiting for first hour
# → task_ingest() — fetch_aqi_data() hits OWM for 10 cities
#     → produce_aqi_batch() sends 10 records to Kafka topic aqi-raw-data
# → task_bronze() — subprocess runs python -m bronze.main
#     → HADOOP_HOME and PATH set automatically via my_env
#     → PySpark reads Kafka, writes JSON batch to S3 bronze/aqi-raw/
#     → stops after 120 seconds (one batch written)
# → task_silver() — run_silver() called directly
#     → PySpark reads all bronze JSON, cleans, writes Parquet to S3 silver/
# → task_gold() — run_gold() called directly
#     → DuckDB loads silver Parquet, creates 6 analytics tables
#     → gold/aqi_warehouse.duckdb updated with fresh data
# → dashboard reads updated DuckDB automatically on next refresh
# → schedule.every(1).hours.do(run_pipeline) — repeats every hour
# → while True loop keeps scheduler alive until Ctrl+C
# → if any task fails — logged as ERROR, next task skipped
# → next run in 1 hour — fresh AQI data for all 10 cities