# PURPOSE: main.py is the FastAPI application. It exposes HTTP endpoints that the outside world (Airflow, my browser, other services) can call to trigger AQI data fetching.

# steps -> create FastAPI app instance
# step 1: Create FastAPI app instance
# step 2: /health endpoint — is service alive?
# step 3: /fetch-aqi endpoint — go get real data
# step 4: /status endpoint — when did we last fetch?


from fastapi import FastAPI, HTTPException
from datetime import datetime
from ingestion.api.aqi_fetcher import fetch_aqi_data
from config.settings import DATA_GOV_API_KEY


app = FastAPI(
    title="India AQI Ingestion API",
    description="Fetches live AQI data from Indian cities",
    version="1.0.0",
)
last_fetch_time = None



@app.get("/health")
def health_check():
    # Confirms service is running and API key is loaded
    return {"status": "ok", "api_key_loaded": bool(DATA_GOV_API_KEY)}


@app.post("/fetch-aqi")
def trigger_fetch(limit: int = 100):
    # Triggers AQI fetch and returns validated records
    global last_fetch_time
    records = fetch_aqi_data(limit=limit)
    if not records:
        raise HTTPException(status_code=500, detail="Fetch failed or returned no data")
    last_fetch_time = datetime.now()
    return {"fetched_at": str(last_fetch_time), "record_count": len(records), "records": [r.model_dump() for r in records]}



@app.get("/status")
def get_status():
    # Returns last successful fetch time and service status
    
    return {
        "last_fetch_time": str(last_fetch_time),
        "status": "never fetched" if not last_fetch_time else "active"
    }






# ── HOW main.py WORKS ──────────────────────────────
#
# uvicorn starts → FastAPI app created
#    ↓
# app = FastAPI(...) → web server is live at localhost:8000
#    ↓
# 3 endpoints registered and waiting for requests:
#    ↓
# GET /health called
#    ↓
# health_check() runs → checks API key loaded
#    ↓
# returns {"status": "ok", "api_key_loaded": True}
#    ↓
# POST /fetch-aqi called
#    ↓
# trigger_fetch() runs → calls fetch_aqi_data()
#    ↓
# fetch_aqi_data() hits data.gov.in API
#    ↓
# 100 city records returned → each validated by AQIRecord schema
#    ↓
# last_fetch_time updated → records returned as JSON
#    ↓
# if fetch fails → HTTPException 500 returned immediately
#    ↓
# GET /status called
#    ↓
# get_status() runs → returns last_fetch_time and service state
#    ↓
# Done!