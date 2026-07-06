# PURPOSE — Reads raw AQI messages from Kafka topic aqi-raw-data using PySpark Structured Streaming and returns a DataFrame.

# steps
# step 1: read_from_kafka() — connects PySpark to Kafka topic, returns raw stream
# step 2: parse_kafka_messages() — extracts the actual JSON string from Kafka message wrapper



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, cast
import config.settings as settings_module



def read_from_kafka(spark: SparkSession):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings_module.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", settings_module.KAFKA_TOPIC_AQI) \
        .option("startingOffsets", "earliest") \
        .load()



def parse_kafka_messages(df):
    return df.select(      #  value (bytes) | topic | partition | offset | timestamp
        col("value").cast("string").alias("raw_json"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    )





# HOW kafka_consumer.py WORKS:
# spark session passed in from bronze/main.py
# → read_from_kafka() connects PySpark to Kafka at localhost:9092
# → subscribes to topic "aqi-raw-data"
# → startingOffsets=earliest — reads ALL messages from beginning
# → returns raw streaming DataFrame with Kafka metadata columns
# → parse_kafka_messages() called on raw DataFrame
# → value column (bytes) cast to string → our AQI JSON appears
# → kafka_timestamp, partition, offset kept for debugging
# → returns clean DataFrame with raw_json column ready for s3_writer.py