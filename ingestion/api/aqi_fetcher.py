import requests
import logging
import certifi
from datetime import datetime
from typing import List

from ingestion.schemas.aqi_schema import AQIRecord
from config.settings import DATA_GOV_API_KEY, OPENWEATHER_API_KEY


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# data.gov.in endpoint — kept for when server is reachable
DATA_GOV_BASE_URL = "https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"

# OpenWeatherMap air pollution endpoint — primary source
OWM_BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution"

# 10 major Indian cities with GPS coordinates for OpenWeatherMap
INDIAN_CITIES = [
    {"city": "Delhi", "state": "Delhi", "lat": 28.6, "lon": 77.2},
    {"city": "Mumbai", "state": "Maharashtra", "lat": 19.0, "lon": 72.8},
    {"city": "Kolkata", "state": "West Bengal", "lat": 22.5, "lon": 88.3},
    {"city": "Chennai", "state": "Tamil Nadu", "lat": 13.0, "lon": 80.2},
    {"city": "Bengaluru", "state": "Karnataka", "lat": 12.9, "lon": 77.5},
    {"city": "Hyderabad", "state": "Telangana", "lat": 17.3, "lon": 78.4},
    {"city": "Patna", "state": "Bihar", "lat": 25.5, "lon": 85.1},
    {"city": "Lucknow", "state": "Uttar Pradesh", "lat": 26.8, "lon": 80.9},
    {"city": "Ahmedabad", "state": "Gujarat", "lat": 23.0, "lon": 72.5},
    {"city": "Jaipur", "state": "Rajasthan", "lat": 26.9, "lon": 75.7},
]

# Official CPCB AQI category breakpoints
AQI_CATEGORY_BREAKPOINTS = {
    "Good": (0, 50),
    "Satisfactory": (51, 100),
    "Moderate": (101, 200),
    "Poor": (201, 300),
    "Very Poor": (301, 400),
    "Hazardous": (401, 500),
}


def get_aqi_category(aqi_value: float) -> str:
    # Returns CPCB category label for a given AQI number
    for category, (low, high) in AQI_CATEGORY_BREAKPOINTS.items():
        if low <= aqi_value <= high:
            return category
    return "Beyond Scale"



def fetch_aqi_data(limit: int = 10) -> List[AQIRecord]:
    # Fetch live AQI from OpenWeatherMap for major Indian cities
    logger.info(f"Fetching AQI data via OpenWeatherMap, cities={limit}")
    records = []
    cities = INDIAN_CITIES[:limit]
    for city_info in cities:
        try:
            params = {
                "lat": city_info["lat"],
                "lon": city_info["lon"],
                "appid": OPENWEATHER_API_KEY,
            }
            response = requests.get(OWM_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API call failed for {city_info['city']}: {e}")
            continue
        try:
            components = data["list"][0]["components"]
            # Convert OWM AQI scale (1-5) to CPCB scale (0-500)
            owm_aqi = data["list"][0]["main"]["aqi"]
            # proper OWM -> CPCB scale mapping (not simple x100)
            OWM_TO_CPCB = {1: 35, 2: 75, 3: 150, 4: 250, 5: 400}
            aqi_value = float(OWM_TO_CPCB.get(owm_aqi, 100))
            record = AQIRecord(
                city=city_info["city"],
                state=city_info["state"],
                aqi=aqi_value,
                timestamp=datetime.now(),
                pm25=components.get("pm2_5"),
                pm10=components.get("pm10"),
                no2=components.get("no2"),
                so2=components.get("so2"),
                aqi_category=get_aqi_category(aqi_value),
            )
            records.append(record)
            logger.info(f"Fetched {city_info['city']}: AQI={aqi_value}")
        except Exception as e:
            logger.warning(f"Skipping {city_info['city']}: {e}")
    return records





# ── HOW aqi_fetcher.py WORKS ─────────────────────────
# fetch_aqi_data() called with limit=10
#    ↓
# Loops through 10 Indian cities
#    ↓
# For each city → builds params with lat, lon, API key
#    ↓
# requests.get() → hits OpenWeatherMap air pollution API
#    ↓
# Response parsed → components extracted (PM2.5, PM10, NO2, SO2)
#    ↓
# OWM AQI (1-5) converted to CPCB scale (100-500)
#    ↓
# AQIRecord created → Pydantic validates all fields
#    ↓
# get_aqi_category() → assigns "Good/Moderate/Poor" label
#    ↓
# Record appended to list → next city
#    ↓
# If any city fails → logged and skipped, others continue
#    ↓
# Returns complete list of validated AQIRecord objects
#    ↓
# Done!