# PURPOSE — Ties all bronze layer files together. Creates Spark session, reads from Kafka, parses messages, writes to S3. This is the file you run to start the bronze pipeline.
# WHY — Each file does one job. main.py is the conductor that calls them all in the right order.




import logging
from bronze.spark_session import create_spark_session
from bronze.kafka_consumer import read_from_kafka, parse_kafka_messages
from bronze.s3_writer import write_to_s3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def main():
    logger.info("Starting AQI Bronze Layer pipeline...")
    spark = create_spark_session()
    raw_df = read_from_kafka(spark)
    parsed_df = parse_kafka_messages(raw_df)    #extracts JSON string from Kafka message wrapper
    logger.info("Kafka stream connected. Writing to S3...")
    query = parsed_df.writeStream \
        .foreachBatch(write_to_s3) \
        .option("checkpointLocation", "s3a://india-aqi-data-lake/bronze/checkpoints/") \
        .trigger(processingTime="30 seconds") \
        .start()
    query.awaitTermination()




if __name__ == "__main__":
    main()





# HOW bronze/main.py WORKS:
# python bronze/main.py called from terminal
# → logging configured — INFO level messages visible in terminal
# → create_spark_session() — Spark starts with Kafka connector jar
# → read_from_kafka(spark) — PySpark connects to Kafka at localhost:9092
# → subscribes to topic "aqi-raw-data", reads from earliest offset
# → parse_kafka_messages() — extracts raw_json from Kafka value bytes
# → writeStream.foreachBatch(write_to_s3) — for every 30 second batch:
#     → write_to_s3() called with batch DataFrame + epoch_id
#     → batch written as JSON file to s3a://bucket/bronze/aqi-raw/
# → checkpointLocation saves progress — safe to restart pipeline
# → query.awaitTermination() — runs forever until Ctrl+C
# → silver layer reads from s3://bucket/bronze/aqi-raw/ in Phase 4




# OpenWeatherMap API
#       ↓
# aqi_fetcher.py (10 cities fetched)
#       ↓
# producer.py (sent to Kafka topic)
#       ↓
# aqi-kafka (Docker container)
#       ↓
# bronze/main.py (PySpark reads Kafka)
#       ↓
# S3: india-aqi-data-lake/bronze/aqi-raw/ 