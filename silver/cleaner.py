# PURPOSE — Takes the raw bronze DataFrame and cleans it: removes nulls, fixes data types, removes duplicates, adds a processed timestamp.



from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp, round, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from datetime import datetime
import logging

logger = logging.getLogger(__name__)



# Step 1: parse the raw JSON into columns, parse means convert JSON string into a struct column because that's how Spark works 
def parse_raw_json(df: DataFrame) -> DataFrame:
    schema = StructType([    # tells Spark what fields to expect inside the JSON string
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("aqi", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("so2", DoubleType(), True),
        StructField("aqi_category", StringType(), True),
    ])
    df = df.withColumn("parsed", from_json(col("raw_json"), schema))   # parses the JSON string into a struct column
    df = df.select("parsed.*", "kafka_timestamp")   # expands the struct into individual columns (city, aqi, pm25 etc.)
    logger.info("Raw JSON parsed into columns.")
    return df



# Step 2: remove nulls
def remove_nulls(df: DataFrame) -> DataFrame:
    required_cols = ["city", "state", "aqi", "timestamp"]
    before = df.count()
    df = df.dropna(subset=required_cols)
    after = df.count()
    logger.info(f"Null removal: {before - after} rows dropped. {after} rows remaining.")
    return df



# Step 3: fix data types
def fix_data_types(df: DataFrame) -> DataFrame:
    df = df.withColumn("aqi", col("aqi").cast("double"))   # replaces the aqi column with itself cast to double (decimal number)
    df = df.withColumn("pm25", col("pm25").cast("double"))
    df = df.withColumn("pm10", col("pm10").cast("double"))
    df = df.withColumn("no2", col("no2").cast("double"))
    df = df.withColumn("so2", col("so2").cast("double"))
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))   # converts timestamp string like "2026-06-28 10:15:00" to a proper Spark TimestampType
    logger.info("Data types fixed.")
    return df



# Step 4: remove duplicates
def remove_duplicates(df: DataFrame) -> DataFrame:
    before = df.count()
    df = df.dropDuplicates(["city", "timestamp"])  # We use city + timestamp together — same city at different times is valid data, not a duplicate. Duplicates happen because we ran the pipeline multiple times — same Kafka messages got written to S3 twice
    after = df.count()   
    logger.info(f"Duplicate removal: {before - after} rows dropped. {after} rows remaining.")
    return df



# Step 5: the main cleaner that ties all steps together
def clean_bronze_data(df: DataFrame) -> DataFrame:
    logger.info("Starting silver cleaning pipeline...")
    df = parse_raw_json(df)
    df = remove_nulls(df)
    df = fix_data_types(df)
    df = remove_duplicates(df)
    df = df.withColumn("silver_processed_at", lit(str(datetime.now())))   # adds a timestamp column so we know when this data was last cleaned and ready for gold layer to read 
    logger.info("Silver cleaning complete.")
    return df





# HOW cleaner.py WORKS:
# clean_bronze_data() called with raw bronze DataFrame from reader.py
# → parse_raw_json() — raw_json column contains JSON string from Kafka
#     → schema defined — tells Spark what fields to expect inside the string
#     → from_json() parses string into struct column called "parsed"
#     → select("parsed.*") expands struct into individual columns (city, aqi, pm25...)
# → remove_nulls() — drops rows where city/state/aqi/timestamp is null
#     → pm25/pm10/no2/so2 allowed to be null — not required fields
#     → logs how many rows dropped
# → fix_data_types() — JSON stores everything as string by default
#     → casts aqi/pm25/pm10/no2/so2 to double so math works correctly
#     → casts timestamp string to proper TimestampType for time queries
# → remove_duplicates() — runs AFTER type fix so timestamp comparison is accurate
#     → drops rows with same city + timestamp combination
#     → same city at different times = valid data, not a duplicate
#     → logs how many duplicates removed
# → withColumn("silver_processed_at") — records when this row was cleaned
# → returns clean DataFrame ready for s3_writer.py
