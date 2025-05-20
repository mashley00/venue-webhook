from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Union, Optional
from fuzzywuzzy import fuzz
from datetime import datetime
import pandas as pd
import logging

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VenueGPT")
app = FastAPI(title="Venue Optimization API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Dataset
CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
try:
    df = pd.read_csv(CSV_URL, encoding="utf-8")
    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df["event_day"] = df["event_date"].dt.day_name()
    df["event_time"] = df["event_time"].astype(str).str.strip()
    df["zip_code"] = df.get("zip_code", pd.Series("", index=df.index)).fillna("").astype(str).str.strip().str.zfill(5)
    logger.info(f"Loaded dataset: {df.shape}")
except Exception as e:
    logger.exception("Error loading dataset.")
    raise e

print("✅ App loaded successfully")

# Static mount
app.mount("/static", StaticFiles(directory="static", html=True), name="static")

# Mappings
TOPIC_MAP = {
    "TIR": "taxes_in_retirement_567",
    "EP": "estate_planning_567",
    "SS": "social_security_567"
}

class VORRequest(BaseModel):
    topic: str
    city: str
    state: Optional[str] = None
    miles: Optional[Union[int, float]] = 6.0

def is_true(val):
    return str(val).strip().upper() == "TRUE"

def get_similar_cities(input_city, state, threshold=75):
    normalized_city = input_city.strip().lower()
    candidates = df[df['state'].str.strip().str.upper() == state]['city'].dropna().unique()
    return list({
        city for city in candidates
        if fuzz.token_set_ratio(normalized_city, city.strip().lower()) >= threshold
    })

@app.get("/ping")
async def ping():
    return {"status": "ok"}

@app.get("/market.html", response_class=HTMLResponse)
async def serve_market():
    return FileResponse("static/market.html")

@app.get("/predict.html", response_class=HTMLResponse)
async def serve_predict():
    return FileResponse("static/predict.html")

@app.get("/market-health", response_class=HTMLResponse)
async def market_health(zip: Optional[str] = None, city: Optional[str] = None, state: Optional[str] = None, topic: Optional[str] = None):
    # Basic placeholder response
    html = f"""
    <h2>Market Health Check</h2>
    <p><b>ZIP:</b> {zip}</p>
    <p><b>City:</b> {city}</p>
    <p><b>State:</b> {state}</p>
    <p><b>Topic:</b> {topic}</p>
    <p>(Add logic here later)</p>
    """
    return HTMLResponse(content=html)

@app.get("/predict-cpr", response_class=HTMLResponse)
async def predict_cpr(zip: Optional[str] = None, city: Optional[str] = None, state: Optional[str] = None, topic: Optional[str] = None):
    # Placeholder prediction logic
    html = f"""
    <h2>Predicted CPR</h2>
    <p><b>ZIP:</b> {zip}</p>
    <p><b>City:</b> {city}</p>
    <p><b>State:</b> {state}</p>
    <p><b>Topic:</b> {topic}</p>
    <p>Prediction logic coming soon!</p>
    """
    return HTMLResponse(content=html)

@app.post("/vor")
async def run_vor(request: VORRequest):
    topic_key = request.topic.strip().upper()
    topic = TOPIC_MAP.get(topic_key)
    if not topic:
        raise HTTPException(status_code=400, detail="Invalid topic code. Use TIR, EP, or SS.")

    if request.city.isdigit() and len(request.city) == 5:
        zip_str = request.city.zfill(5)
        filtered = df[(df['topic'] == topic) & (df['zip_code'] == zip_str)]
    else:
        city = request.city.strip()
        state = request.state.strip().upper()
        matches = get_similar_cities(city, state)
        if not matches:
            raise HTTPException(status_code=404, detail="No similar cities found.")
        filtered = df[
            (df['topic'] == topic) &
            (df['city'].str.lower().isin([c.lower() for c in matches])) &
            (df['state'].str.upper() == state)
        ]

    if filtered.empty:
        raise HTTPException(status_code=404, detail="No matching events found.")

    return {
        "message": f"Found {len(filtered)} events for {topic_key} in {request.city}, {request.state}"
    }















