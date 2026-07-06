# PURPOSE — Runs 6 business analytics queries against the aqi_silver DuckDB table and saves results as permanent gold tables.


# step 1: create_top_polluted_cities() — top 5 cities by average AQI
# step 2: create_city_aqi_summary() — min, max, avg per city
# step 3: create_aqi_category_distribution() — national snapshot
# step 4: create_most_dangerous_pollutant() — worst pollutant per city
# step 5: create_hourly_aqi_trend() — AQI by hour of day
# step 6: create_city_health_risk_score() — custom composite score
# step 7: run_all_analytics() — runs all 6 in order



import logging

logger = logging.getLogger(__name__)




# Step 1: top 5 cities by average AQI
def create_top_polluted_cities(conn):
    conn.execute("DROP TABLE IF EXISTS top_polluted_cities;")
    conn.execute("""
        CREATE TABLE top_polluted_cities AS
        SELECT
            city,
            state,
            ROUND(AVG(aqi), 2) AS avg_aqi,
            ROUND(MAX(aqi), 2) AS max_aqi,
            aqi_category
        FROM aqi_silver
        GROUP BY city, state, aqi_category
        ORDER BY avg_aqi DESC
        LIMIT 5;
    """)
    logger.info("Table created: top_polluted_cities")




# Step 2: min, max, avg per city - Complete city health report card — one row per city showing full pollution profile.
def create_city_aqi_summary(conn):
    conn.execute("DROP TABLE IF EXISTS city_aqi_summary;")
    conn.execute("""
        CREATE TABLE city_aqi_summary AS
        SELECT
            city,
            state,
            ROUND(MIN(aqi), 2)  AS min_aqi,
            ROUND(MAX(aqi), 2)  AS max_aqi,
            ROUND(AVG(aqi), 2)  AS avg_aqi,
            ROUND(AVG(pm25), 2) AS avg_pm25,
            ROUND(AVG(pm10), 2) AS avg_pm10,
            aqi_category
        FROM aqi_silver
        GROUP BY city, state, aqi_category
        ORDER BY avg_aqi DESC;
    """)
    logger.info("Table created: city_aqi_summary")




# Step 3: national snapshot of AQI categories - "60% of Indian cities are in Poor or Very Poor category today."
def create_aqi_category_distribution(conn):
    conn.execute("DROP TABLE IF EXISTS aqi_category_distribution;")
    conn.execute("""
        CREATE TABLE aqi_category_distribution AS
        SELECT
            aqi_category,
            COUNT(*) AS city_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
        FROM aqi_silver
        GROUP BY aqi_category
        ORDER BY city_count DESC;
    """)
    logger.info("Table created: aqi_category_distribution")



# Step 4: worst pollutant per city - Health advisories can be targeted — "Delhi residents: PM2.5 is your main threat today, wear N95 masks."
def create_most_dangerous_pollutant(conn):
    conn.execute("DROP TABLE IF EXISTS most_dangerous_pollutant;")
    conn.execute("""
        CREATE TABLE most_dangerous_pollutant AS
        SELECT
            city,
            state,
            ROUND(AVG(pm25), 2) AS avg_pm25,
            ROUND(AVG(pm10), 2) AS avg_pm10,
            ROUND(AVG(no2),  2) AS avg_no2,
            ROUND(AVG(so2),  2) AS avg_so2,
            CASE
                WHEN AVG(pm25) >= AVG(pm10)
                 AND AVG(pm25) >= AVG(no2)
                 AND AVG(pm25) >= AVG(so2) THEN 'PM2.5'
                WHEN AVG(pm10) >= AVG(no2)
                 AND AVG(pm10) >= AVG(so2) THEN 'PM10'
                WHEN AVG(no2)  >= AVG(so2) THEN 'NO2'
                ELSE 'SO2'
            END AS dominant_pollutant
        FROM aqi_silver
        GROUP BY city, state
        ORDER BY avg_pm25 DESC;
    """)
    logger.info("Table created: most_dangerous_pollutant")




# Step 5: AQI by hour of day - "What time of day is safest to go outside?" — 6am might show lowest AQI (best air), 8pm might show highest (worst air after evening traffic and industry).
def create_hourly_aqi_trend(conn):
    conn.execute("DROP TABLE IF EXISTS hourly_aqi_trend;")
    conn.execute("""
        CREATE TABLE hourly_aqi_trend AS
        SELECT
            HOUR(timestamp) AS hour_of_day,
            ROUND(AVG(aqi), 2) AS avg_aqi,
            ROUND(MIN(aqi), 2) AS min_aqi,
            ROUND(MAX(aqi), 2) AS max_aqi,
            COUNT(*) AS reading_count
        FROM aqi_silver
        GROUP BY HOUR(timestamp)
        ORDER BY hour_of_day ASC;
    """)
    logger.info("Table created: hourly_aqi_trend")




# Step 6: create_city_health_risk_score() — custom composite score
# AVG(pm25) * 0.3 — PM2.5 contributes 30% (most dangerous to lungs and heart)
# AVG(no2) * 0.2 — NO2 contributes 20% (causes respiratory disease)
def create_city_health_risk_score(conn):
    conn.execute("DROP TABLE IF EXISTS city_health_risk_score;")
    conn.execute("""
        CREATE TABLE city_health_risk_score AS
        SELECT
            city,
            state,
            ROUND(AVG(aqi), 2)  AS avg_aqi,
            ROUND(AVG(pm25), 2) AS avg_pm25,
            ROUND(AVG(no2), 2)  AS avg_no2,
            ROUND(
                (AVG(aqi) * 0.5) + (AVG(pm25) * 0.3) + (AVG(no2) * 0.2), 2) AS health_risk_score,
            CASE
                WHEN (AVG(aqi)*0.5 + AVG(pm25)*0.3 + AVG(no2)*0.2) >= 250 THEN 'CRITICAL'
                WHEN (AVG(aqi)*0.5 + AVG(pm25)*0.3 + AVG(no2)*0.2) >= 150 THEN 'HIGH'
                WHEN (AVG(aqi)*0.5 + AVG(pm25)*0.3 + AVG(no2)*0.2) >= 75  THEN 'MODERATE'
                ELSE 'LOW'
            END AS risk_level
        FROM aqi_silver
        GROUP BY city, state
        ORDER BY health_risk_score DESC;
    """)
    logger.info("Table created: city_health_risk_score")




# Step 7: run_all_analytics() — runs all 6 in order
def run_all_analytics(conn):
    logger.info("Running all gold analytics...")
    create_top_polluted_cities(conn)
    create_city_aqi_summary(conn)
    create_aqi_category_distribution(conn)
    create_most_dangerous_pollutant(conn)
    create_hourly_aqi_trend(conn)
    create_city_health_risk_score(conn)
    logger.info("All 6 gold analytics tables created successfully.")





# HOW analytics.py WORKS:
# run_all_analytics(conn) called from gold/main.py
# → create_top_polluted_cities() — top 5 cities by average AQI
#     → AVG(aqi) per city, ORDER BY avg_aqi DESC, LIMIT 5
#     → business use: emergency intervention prioritization
# → create_city_aqi_summary() — full profile for all 10 cities
#     → MIN, MAX, AVG of aqi + pm25 + pm10 per city
#     → business use: city health report card
# → create_aqi_category_distribution() — national snapshot
#     → COUNT per category + percentage using window function OVER()
#     → business use: "60% of cities are Poor or Very Poor today"
# → create_most_dangerous_pollutant() — dominant pollutant per city
#     → CASE WHEN compares PM2.5, PM10, NO2, SO2 averages
#     → business use: targeted health advisories
# → create_hourly_aqi_trend() — AQI by hour of day
#     → HOUR(timestamp) extracts hour, AVG/MIN/MAX per hour
#     → business use: "safest time to go outside"
# → create_city_health_risk_score() — custom composite score
#     → formula: AQI*0.5 + PM2.5*0.3 + NO2*0.2
#     → CASE WHEN assigns CRITICAL/HIGH/MODERATE/LOW label
#     → business use: composite health index for dashboard
# → all 6 tables saved permanently in gold/aqi_warehouse.duckdb
