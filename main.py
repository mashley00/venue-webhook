from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime
import requests
import os
import traceback

app = FastAPI()

# Enable CORS if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic request model
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: Optional[float] = 6.0

# Pydantic response model
class VORResponse(BaseModel):
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

@app.post("/vor", response_model=List[VORResponse])
def get_vor(request: VORRequest):
    try:
        # Load CSV from S3
        s3_url = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
        df = pd.read_csv(s3_url)

        # Clean column names
        df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()

        # Parse dates
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')

        # Map topic
        topic_map = {
            "TIR": "taxes_in_retirement_567",
            "EP": "estate_planning_567",
            "SS": "social_security_567"
        }
        topic_mapped = topic_map.get(request.topic.upper())
        if not topic_mapped:
            raise HTTPException(status_code=400, detail="Invalid topic code")

        # Filter
        filtered = df[
            (df['topic'] == topic_mapped) &
            (df['city'].str.lower() == request.city.lower()) &
            (df['state'].str.lower() == request.state.lower())
        ]

        if filtered.empty:
            return []

        # Compute fields
        filtered['attendance_rate'] = filtered['attended_hh'] / filtered['gross_registrants']
        filtered['fulfillment_pct'] = filtered['attended_hh'] / (filtered['registration_max'] / 2.4)
        filtered['cost_per_verified_hh'] = filtered['fb_cpr'] / filtered['attendance_rate']

        # Grouped stats
        grouped = filtered.groupby('venue').agg(
            city=('city', 'first'),
            state=('state', 'first'),
            most_recent_event=('event_date', 'max'),
            number_of_events=('event_date', 'count'),
            avg_gross_registrants=('gross_registrants', 'mean'),
            avg_cpa=('cost_per_verified_hh', 'mean'),
            avg_cpr=('fb_cpr', 'mean'),
            attendance_rate=('attendance_rate', 'mean'),
            fulfillment_pct=('fulfillment_pct', 'mean'),
            image_allowed=('venue_image_allowed', lambda x: x.mode()[0] if not x.empty else False),
            disclosure_needed=('venue_disclosure', lambda x: x.mode()[0] if not x.empty else False)
        ).reset_index()

        # Score
        grouped['score'] = (
            (1 / grouped['avg_cpa']) * 0.5 +
            grouped['fulfillment_pct'] * 0.3 +
            grouped['attendance_rate'] * 0.2
        ) * 100 / 1.0  # Convert to 100-pt scale

        # Sort
        grouped = grouped.sort_values(by='score', ascending=False).head(4)

        # Add time slot recs (placeholder for now)
        grouped['best_time_1'] = "11:00am on Monday"
        grouped['best_time_2'] = "6:00pm on Monday"

        # Format date
        grouped['most_recent_event'] = grouped['most_recent_event'].dt.strftime("%Y-%m-%d")

        # Build response
        response = grouped.to_dict(orient='records')
        return response

    except Exception as e:
        error_message = f"{str(e)}\n\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_message)
















