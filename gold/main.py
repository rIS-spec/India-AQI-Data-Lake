# PURPOSE — Ties all gold layer files together. Connects DuckDB, installs S3 extension, loads silver Parquet, runs all 6 analytics, prints results.



import logging
from gold.duckdb_loader import connect_duckdb, install_s3_extension, load_silver_to_duckdb
from gold.analytics import run_all_analytics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def main():
    logger.info("Starting AQI Gold Layer pipeline...")
    conn = connect_duckdb()
    install_s3_extension(conn)
    load_silver_to_duckdb(conn)
    run_all_analytics(conn)
    logger.info("Printing gold analytics results...")
    tables = ["top_polluted_cities", "city_aqi_summary",
              "aqi_category_distribution", "most_dangerous_pollutant",
              "city_health_risk_score"]
    for table in tables:
        print(f"\n=== {table.upper()} ===")
        print(conn.execute(f"SELECT * FROM {table};").df().to_string())    # .to_string() returns a string representation of the DataFrame - prints all rows without truncation
    conn.close()
    logger.info("Gold pipeline complete.")



if __name__ == "__main__":
    main()






# HOW gold/main.py WORKS:
# python -m gold.main called from project root
# → connect_duckdb() opens/creates gold/aqi_warehouse.duckdb file
# → install_s3_extension() — INSTALL+LOAD httpfs, SET s3 credentials
# → load_silver_to_duckdb() reads s3://india-aqi-data-lake/silver/aqi-clean/*.parquet
#     → DROP TABLE IF EXISTS aqi_silver — clean previous run
#     → CREATE TABLE aqi_silver AS SELECT * FROM read_parquet(...)
#     → logs how many records loaded
# → run_all_analytics() creates 6 gold tables in order:
#     → top_polluted_cities — top 5 by avg AQI
#     → city_aqi_summary — full profile all 10 cities
#     → aqi_category_distribution — national snapshot with percentages
#     → most_dangerous_pollutant — dominant pollutant per city
#     → hourly_aqi_trend — AQI by hour of day
#     → city_health_risk_score — custom weighted risk formula
# → prints 5 tables to terminal as Pandas DataFrames
# → conn.close() — DuckDB connection closed cleanly
# → gold/aqi_warehouse.duckdb file saved to disk permanently
# → dashboard (Phase 10) queries this .duckdb file directly