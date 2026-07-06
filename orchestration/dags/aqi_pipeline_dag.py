# PURPOSE of aqi_pipeline_dag.py — Defines the complete AQI pipeline as an Airflow DAG with 4 tasks running in sequence on a daily schedule.



from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator



default_args = {
    "owner": "arish",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="aqi_pipeline",
    default_args=default_args,
    description="India AQI Data Lake — daily pipeline",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aqi", "data-engineering", "india"],
) as dag:
    
    #Task 1: fetches AQI from OWM and sends to Kafka
    def fetch_and_produce():
        from ingestion.api.aqi_fetcher import fetch_aqi_data
        from ingestion.kafka.producer import produce_aqi_batch
        records = [r.model_dump() for r in fetch_aqi_data()]
        produce_aqi_batch(records)

    # Task 2: starts PySpark streaming, writes to S3 (subprocess because streaming)
    def run_bronze():
        import subprocess
        subprocess.run(["python", "-m", "bronze.main"], timeout=300)

    # Task 3: reads bronze, cleans, writes Parquet to S3
    def run_silver():
        from silver.main import main
        main()

    # Task 4: loads silver into DuckDB, creates 6 analytics tables
    def run_gold():
        from gold.main import main
        main()



    task_ingest = PythonOperator(
        task_id="fetch_and_produce",
        python_callable=fetch_and_produce,
    )

    task_bronze = PythonOperator(
        task_id="run_bronze",
        python_callable=run_bronze,
    )

    task_silver = PythonOperator(
        task_id="run_silver",
        python_callable=run_silver,
    )

    task_gold = PythonOperator(
        task_id="run_gold",
        python_callable=run_gold,
    )

    task_ingest >> task_bronze >> task_silver >> task_gold




# HOW aqi_pipeline_dag.py WORKS:
# Airflow scheduler reads this file every 30 seconds
# → DAG "aqi_pipeline" scheduled at 6:00 AM daily (cron: 0 6 * * *)
# → At 6:00 AM Airflow triggers task_ingest:
#     → fetch_and_produce() — OWM API fetches 10 cities → Kafka topic
# → task_ingest SUCCESS → task_bronze starts:
#     → run_bronze() — PySpark reads Kafka → writes JSON to S3 bronze/
# → task_bronze SUCCESS → task_silver starts:
#     → run_silver() — PySpark reads S3 bronze → cleans → Parquet to S3 silver/
# → task_silver SUCCESS → task_gold starts:
#     → run_gold() — DuckDB reads S3 silver → 6 analytics tables created
# → All tasks SUCCESS → DAG run marked complete in Airflow UI
# → If any task FAILS → retried once after 5 minutes
# → Next run: tomorrow at 6:00 AM
# → Dashboard reads updated gold/aqi_warehouse.duckdb automatically