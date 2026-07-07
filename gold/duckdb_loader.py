# PURPOSE — Connects to a local DuckDB database file, installs the S3 extension and loads the clean Parquet data from S3 silver layer directly into DuckDB as a table.


# step 1: connect_duckdb() — creates/opens local DuckDB file
# step 2: install_s3_extension() — gives DuckDB ability to read from AWS S3
# step 3: load_silver_to_duckdb() — reads Parquet from S3 into DuckDB table


import duckdb
import config.settings as settings_module
import logging

logger = logging.getLogger(__name__)



def connect_duckdb():
    db_path = settings_module.DUCKDB_PATH
    conn = duckdb.connect(db_path)
    logger.info(f"DuckDB connected at: {db_path}")
    return conn     # the connection object all other functions will use to run SQL queries


def install_s3_extension(conn):
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_region='{settings_module.AWS_REGION}';")
    conn.execute(f"SET s3_access_key_id='{settings_module.AWS_ACCESS_KEY_ID}';")
    conn.execute(f"SET s3_secret_access_key='{settings_module.AWS_SECRET_ACCESS_KEY}';")
    logger.info("S3 extension installed and configured.")



def load_silver_to_duckdb(conn):
    silver_path = f"s3://{settings_module.S3_BUCKET_NAME}/silver/aqi-clean/*.parquet"
    logger.info(f"Loading silver data from: {silver_path}")
    conn.execute("DROP TABLE IF EXISTS aqi_silver;")
    conn.execute(f"""
        CREATE TABLE aqi_silver AS
        SELECT * FROM read_parquet('{silver_path}');
    """)      #reads Parquet directly from S3 and creates a DuckDB table in one SQL statement
    count = conn.execute("SELECT COUNT(*) FROM aqi_silver;").fetchone()[0]     # gets the count number from the query result
    logger.info(f"aqi_silver table created with {count} records.")






# HOW duckdb_loader.py WORKS:
# connect_duckdb() called — opens/creates gold/aqi_warehouse.duckdb file
# → DUCKDB_PATH from .env: gold/aqi_warehouse.duckdb
# → file persists between sessions — gold tables survive restarts
# → returns conn object used by all other functions
# → install_s3_extension() configures DuckDB to read from AWS S3
#     → INSTALL + LOAD httpfs — teaches DuckDB to read S3 files
#     → SET s3_region, s3_access_key_id, s3_secret_access_key — AWS auth
# → load_silver_to_duckdb() reads Parquet from S3 into DuckDB table
#     → silver_path: s3://india-aqi-data-lake/silver/aqi-clean/*.parquet
#     → DROP TABLE IF EXISTS — cleans up previous run's table
#     → CREATE TABLE aqi_silver AS SELECT * FROM read_parquet(...)
#     → one SQL statement reads S3 Parquet and creates table simultaneously
#     → logs how many records loaded
# → aqi_silver table now ready for analytics.py to query