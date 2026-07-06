# PURPOSE — Takes the cleaned DataFrame from cleaner.py and writes it as Parquet files to S3 silver layer.
# WHY — Silver uses Parquet format instead of JSON. Parquet is columnar — reads 10x faster for analytics, compresses 5x better than JSON and preserves data types properly.



import config.settings as settings_module
import logging

logger = logging.getLogger(__name__)



def write_to_silver(df, mode="overwrite"):    # silver CAN overwrite because it's always rebuilt fresh from bronze
    silver_path = f"s3a://{settings_module.S3_BUCKET_NAME}/silver/aqi-clean/"
    logger.info(f"Writing silver data to: {silver_path}")
    df.write \
        .mode(mode) \
        .parquet(silver_path)    # writes as Parquet instead of JSON — faster reads, smaller files, preserves types
    logger.info(f"Silver write complete. Mode: {mode}")






# HOW writer.py WORKS:
# write_to_silver() called with clean DataFrame from cleaner.py
# → silver_path built using S3_BUCKET_NAME from .env
# → path: s3a://india-aqi-data-lake/silver/aqi-clean/
# → df.write.mode("overwrite") — silver is always rebuilt fresh from bronze
# → .parquet() — writes as Parquet format (not JSON like bronze)
# → Parquet benefits over JSON:
#     → columnar format — reads only needed columns, 10x faster analytics
#     → compression — 5x smaller file size than JSON
#     → type preservation — double stays double, timestamp stays timestamp
# → silver layer now ready for gold layer (DuckDB) to read in Phase 7