# PURPOSE — Takes the parsed streaming DataFrame from kafka_consumer.py and writes each batch as a raw JSON file to AWS S3.
# WHY — Kafka is temporary storage — messages get deleted after a retention period (default 7 days). S3 is permanent. This file moves data from temporary to permanent storage.




import config.settings as settings_module
import logging

logger = logging.getLogger(__name__)




def write_to_s3(df, epoch_id):
    s3_path = f"s3a://{settings_module.S3_BUCKET_NAME}/bronze/aqi-raw/"
    logger.info(f"Writing batch {epoch_id} to S3: {s3_path}")
    df.write \
        .mode("append") \
        .json(s3_path)
    logger.info(f"Batch {epoch_id} written successfully.")




# HOW s3_writer.py WORKS:
# PySpark streaming engine calls write_to_s3() for every new batch
# → epoch_id increments each batch (0, 1, 2...)
# → s3_path built using S3_BUCKET_NAME from .env
# → df.write.mode("append") — never overwrites existing files
# → .json() writes batch as raw JSON file to S3 bronze/aqi-raw/ folder
# → each batch becomes one JSON file in S3
# → silver layer reads from this same S3 path in Phase 4