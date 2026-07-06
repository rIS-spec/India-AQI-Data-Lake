# PURPOSE — Reads raw JSON files from S3 bronze layer into a PySpark DataFrame.


from pyspark.sql import SparkSession
import config.settings as settings_module
import logging

logger = logging.getLogger(__name__)


def read_bronze_data(spark: SparkSession):
    bronze_path = f"s3a://{settings_module.S3_BUCKET_NAME}/bronze/aqi-raw/"
    logger.info(f"Reading bronze data from: {bronze_path}")
    return spark.read.json(bronze_path)      #reads ALL JSON files in bronze/aqi-raw/ in that folder into one DataFrame


