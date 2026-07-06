# PURPOSE — Loads AQI data from DuckDB gold layer into a Pandas DataFrame ready for ML training.
# WHY — LSTM needs historical sequences of AQI values to learn patterns. The gold layer already has clean, aggregated city data — we read it directly from DuckDB instead of hitting S3 again.


# step - load_aqi_data() — reads city_aqi_summary from DuckDB



import duckdb
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def load_aqi_data():
    conn = duckdb.connect("gold/aqi_warehouse.duckdb")
    df = conn.execute("""
        SELECT city, avg_aqi, avg_pm25, avg_pm10,
               aqi_category
        FROM city_aqi_summary
        ORDER BY city, avg_aqi
    """).df()   
    conn.close()
    logger.info(f"Loaded {len(df)} records from DuckDB.")
    return df


