from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import requests
import os

app = FastAPI()

# Define the request model
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: int = 6

# Define the response model (optional strict validation for later)
class VenueRecommendation(BaseModel):
    venue: str
    city: str
    state: str
    most_recent_event: str
    number_of_events: int
    avg_gross_registrants: float
    avg_cpa: float
    avg_cpr: float
    attendance_rate: float
    fulfillment_pct: float
    image_allowed: bool
    disclosure_needed: bool
    score: float
    best_time_1: str
    best_time_2: str

# Load the dataset from S3
S3_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

def load_data():
    try:
        df = pd.read_csv(S3_URL)
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to load data: {e}")

# Main POST route
@app.post("/vor")
async def get_vor(payload: VORRequest):
    try:
        df = load_data()

        # Map short topic codes to full dataset topic names
        topic_map = {
            "TIR": "taxes_in_retirement_567",
            "EP": "estate_planning_567",
            "SS": "social_security_567"
        }

        topic_code = payload.topic.upper().strip()
        topic_key = topic_map.get(topic_code)

        if not topic_key:
            raise HTTPException(status_code=400, detail=f"Invalid topic code '{topic_code}'. Must be one of: {list(topic_map.keys())}")

        # Filter based on topic, city, and state
        df_filtered = df[
            (df["Topic"] == topic_key) &
            (df["City"].str.lower() == payload.city.lower()) &
            (df["State"].str.upper() == payload.state.upper())
        ]

        if df_filtered.empty:
            return [{"error": f"No events found for {payload.city}, {payload.state} with topic {payload.topic}"}]

        # Placeholder scoring logic (to be replaced with real model)
        df_filtered["score"] = 1  # Dummy value for now

        # Simplified return for initial test
        return df_filtered[["Venue", "City", "State", "Topic"]].head(4).to_dict(orient="records")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
















