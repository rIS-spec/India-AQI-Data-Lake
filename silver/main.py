# PURPOSE — Ties all silver files together. Creates Spark session, reads bronze data, cleans it, writes to silver. Run this one file to execute the entire silver pipeline.
# WHY — Each file does one job. main.py is the conductor that calls them all in the right order.




import logging
from silver.spark_session import create_silver_spark_session
from silver.reader import read_bronze_data
from silver.cleaner import clean_bronze_data
from silver.writer import write_to_silver

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




def main():
    logger.info("Starting AQI Silver Layer pipeline...")
    spark = create_silver_spark_session()
    raw_df = read_bronze_data(spark)
    logger.info(f"Bronze records loaded: {raw_df.count()}")
    clean_df = clean_bronze_data(raw_df)
    write_to_silver(clean_df)
    logger.info("Silver pipeline complete.")
    spark.stop()     # silver is a batch job not streaming, so we stop Spark when done, Unlike bronze which runs forever, silver runs once, finishes, and exits cleanly




if __name__ == "__main__":
    main()






# HOW silver/main.py WORKS:
# python -m silver.main called from project root
# → create_silver_spark_session() starts PySpark with S3 jars only (no Kafka)
# → read_bronze_data() reads ALL JSON files from s3a://india-aqi-data-lake/bronze/aqi-raw/
#     → combines all batches into one single DataFrame
# → raw_df.count() logged — shows how many raw records came from bronze
# → clean_bronze_data() runs 4 steps in order:
#     → remove_nulls() — drops incomplete rows
#     → fix_data_types() — casts strings to double and timestamp
#     → remove_duplicates() — drops same city + timestamp duplicates
#     → adds silver_processed_at column
# → write_to_silver() writes clean DataFrame as Parquet to:
#     → s3a://india-aqi-data-lake/silver/aqi-clean/
# → spark.stop() — batch job ends cleanly
# → gold layer reads from silver/aqi-clean/ in Phase 7