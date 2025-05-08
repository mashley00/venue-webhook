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
app = FastAPI()

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

# --- Pydantic Input Model ---
class VORRequest(BaseModel):
    topic: str = Field(..., description="Seminar topic code: TIR, EP, or SS")
    city: str
    state: str
    miles: Optional[Union[int, float]] = 6.0

# --- Utility for filtering and scoring ---
def calculate_scores(filtered_df: pd.DataFrame) -> pd.DataFrame:
    # Defensive copy
    df = filtered_df.copy()

    # Drop rows with missing required fields
    required_cols = ['attended_hh', 'gross_registrants', 'registration_max', 'fb_cpr']
    df.dropna(subset=required_cols, inplace=True)

    # Metric Calculations
    df['attendance_rate'] = df['attended_hh'] / df['gross_registrants']
    df['fulfillment_pct'] = df['attended_hh'] / (df['registration_max'] / 2.4)
    df['score'] = (1 / df['fb_cpr'] * 0.5) + (df['fulfillment_pct'] * 0.3) + (df['attendance_rate'] * 0.2)
    df['score'] = df['score'] * 40

    return df.sort_values(by='score', ascending=False)

# --- Endpoint ---
@app.post("/vor")
async def run_vor(request: VORRequest):
    logger.info(f"Received /vor request: {request.dict()}")

    try:
        topic = request.topic.upper()
        city = request.city.lower()
        state = request.state.upper()
        miles = float(request.miles)

        # Filter logic — basic example by city/state/topic
        filtered = df[
            (df['topic'] == topic) &
            (df['city'].str.lower() == city) &
            (df['state'].str.upper() == state)
        ]

        if filtered.empty:
            raise HTTPException(status_code=404, detail="No matching events found.")

        scored = calculate_scores(filtered)

        result = scored.head(4)[[
            'venue', 'event_date', 'gross_registrants', 'attended_hh', 'fb_cpr',
            'attendance_rate', 'fulfillment_pct', 'score', 'venue_image_allowed', 'venue_disclosure_needed'
        ]]

        return result.to_dict(orient="records")

    except Exception as e:
        logger.exception("Failed to process VOR request.")
        raise HTTPException(status_code=500, detail=str(e))







