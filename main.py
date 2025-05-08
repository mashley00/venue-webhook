from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Union, Optional
import pandas as pd
import logging

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VenueGPT")

# --- FastAPI App Init ---
app = FastAPI(
    title="Venue Optimization API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# --- Load Data from S3 on Startup ---
CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

try:
    df = pd.read_csv(CSV_URL, encoding="utf-8")
    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
    logger.info(f"Loading data from: {CSV_URL}")
    logger.info(f"Data shape after load: {df.shape}")
except Exception as e:
    logger.exception("Failed to load or process CSV file.")
    raise e

# --- Topic Maps ---
TOPIC_MAP = {
    "TIR": "taxes_in_retirement_567",
    "EP": "estate_planning_567",
    "SS": "social_security_567"
}
TOPIC_MAP_REVERSE = {
    "taxes in retirement": "TIR",
    "taxes_in_retirement_567": "TIR",
    "estate planning": "EP",
    "estate_planning_567": "EP",
    "social security": "SS",
    "social_security_567": "SS"
}

# --- Pydantic Input Model ---
class VORRequest(BaseModel):
    topic: str = Field(..., description="Seminar topic code: TIR, EP, or SS")
    city: str
    state: str
    miles: Optional[Union[int, float]] = 6.0

# --- Health Check Endpoint ---
@app.get("/health")
async def health_check():
    return {"status": "ok", "message": "VenueGPT API is live."}

# --- Utility for filtering and scoring ---
def calculate_scores(filtered_df: pd.DataFrame) -> pd.DataFrame:
    df = filtered_df.copy()
    required_cols = ['attended_hh', 'gross_registrants', 'registration_max', 'fb_cpr']
    df.dropna(subset=required_cols, inplace=True)

    df['attendance_rate'] = df['attended_hh'] / df['gross_registrants']
    df['fulfillment_pct'] = df['attended_hh'] / (df['registration_max'] / 2.4)
    df['score'] = (1 / df['fb_cpr'] * 0.5) + (df['fulfillment_pct'] * 0.3) + (df['attendance_rate'] * 0.2)
    df['score'] = df['score'] * 40

    return df.sort_values(by='score', ascending=False)

# --- Main Endpoint ---
@app.post("/vor", summary="Get Venue Optimization Report")
async def run_vor(request: VORRequest):
    logger.info(f"Received /vor request: {request.dict()}")

    try:
        input_topic = request.topic.strip()
        topic_key = input_topic.upper()

        # Handle full name or code
        if topic_key not in TOPIC_MAP:
            simplified = input_topic.lower().replace("_", " ").strip()
            fallback_code = TOPIC_MAP_REVERSE.get(simplified)
            topic_key = fallback_code or topic_key

        topic = TOPIC_MAP.get(topic_key)

        if not topic:
            raise HTTPException(status_code=400, detail="Invalid topic code. Use TIR, EP, or SS.")

        city = request.city.strip().lower()
        state = request.state.strip().upper()
        miles = float(request.miles)

        filtered = df[
            (df['topic'].str.strip().str.lower() == topic.lower()) &
            (df['city'].str.strip().str.lower() == city) &
            (df['state'].str.strip().str.upper() == state)
        ]

    except Exception as e:
        logger.exception("Failed during filtering.")
        raise HTTPException(status_code=500, detail=f"Filtering error: {str(e)}")

    if filtered.empty:
        logger.warning("No matching events found after filtering.")
        raise HTTPException(status_code=404, detail="No matching events found.")

    try:
        scored = calculate_scores(filtered)

        result = scored.head(4)[[
            'venue', 'event_date', 'gross_registrants', 'attended_hh', 'fb_cpr',
            'attendance_rate', 'fulfillment_pct', 'score',
            'image_allowed', 'venue_disclosure'
        ]]

        return result.to_dict(orient="records")

    except Exception as e:
        logger.exception("Failed during scoring or result formatting.")
        raise HTTPException(status_code=500, detail=f"Scoring error: {str(e)}")









