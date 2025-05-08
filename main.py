import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import datetime
import math
import logging

# -------------------------
# Logging Setup
# -------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VenueGPT")

# -------------------------
# Constants
# -------------------------
S3_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
VERSION = "VenueGPT v1.0.0 - 2025-05-08"

# -------------------------
# FastAPI App
# -------------------------
app = FastAPI(title="Venue Optimization API", version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# Response Model
# -------------------------
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

# -------------------------
# Utility Functions
# -------------------------
def weight_by_recency(date, reference):
    days_old = (reference - date).days
    if days_old <= 30:
        return 1.25
    elif days_old <= 90:
        return 1.0
    else:
        return 0.8

def best_times(times: List[str]) -> List[str]:
    preferred = ["11:00 AM", "11:30 AM", "6:00 PM", "6:30 PM"]
    filtered = [t for t in times if t in preferred]
    return (filtered + times[:2])[:2] if len(filtered) < 2 else filtered[:2]

# -------------------------
# Data Loading
# -------------------------
try:
    logger.info(f"Loading data from: {S3_URL}")
    df = pd.read_csv(S3_URL)
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
    logger.info(f"Data shape after load: {df.shape}")
except Exception as e:
    logger.exception("Error loading dataset from S3")
    raise RuntimeError(f"Failed to load data: {e}")

# -------------------------
# POST Endpoint
# -------------------------
@app.post("/vor", response_model=List[VenueRecommendation])
def get_vor(
    topic: str = Query(..., description="Seminar topic (TIR, EP, or SS)"),
    city: str = Query(...),
    state: str = Query(...),
    miles: Optional[float] = Query(6.0)
):
    logger.info(f"Received VOR request for {topic=} {city=}, {state=}, {miles=}")
    reference_date = datetime.datetime.now()

    topic_map = {
        "TIR": "taxes_in_retirement_567",
        "EP": "estate_planning_567",
        "SS": "social_security_567"
    }

    if topic not in topic_map:
        logger.warning("Invalid topic provided")
        raise HTTPException(status_code=400, detail="Invalid topic. Must be one of: TIR, EP, SS")

    topic_value = topic_map[topic]

    try:
        df_filtered = df[
            (df['topic'].str.lower() == topic_value.lower()) &
            (df['city'].str.lower() == city.lower()) &
            (df['state'].str.lower() == state.lower()) &
            (df['event_date'].notnull())
        ].copy()

        logger.info(f"Filtered data shape: {df_filtered.shape}")

        if df_filtered.empty:
            logger.warning("No data found for given filter criteria")
            raise HTTPException(status_code=404, detail="No matching venue data found.")

        required_cols = ['attended_hh', 'gross_registrants', 'registration_max', 'fb_cpr', 'event_time', 'image_allowed', 'venue_disclosure']
        missing_cols = [col for col in required_cols if col not in df_filtered.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            raise HTTPException(status_code=500, detail=f"Missing columns: {missing_cols}")

        df_filtered['attendance_rate'] = df_filtered['attended_hh'] / df_filtered['gross_registrants']
        df_filtered['fulfillment_pct'] = df_filtered['attended_hh'] / (df_filtered['registration_max'] / 2.4)
        df_filtered['cost_per_verified_hh'] = df_filtered['fb_cpr'] / df_filtered['attendance_rate']
        df_filtered['recency_weight'] = df_filtered['event_date'].apply(lambda d: weight_by_recency(d, reference_date))

        df_filtered = df_filtered.replace([float("inf"), -float("inf")], pd.NA).dropna(
            subset=["cost_per_verified_hh", "attendance_rate", "fulfillment_pct", "fb_cpr"]
        )

        grouped = df_filtered.groupby('venue').agg({
            'event_date': 'max',
            'attended_hh': 'count',
            'gross_registrants': 'mean',
            'cost_per_verified_hh': 'mean',
            'fb_cpr': 'mean',
            'attendance_rate': 'mean',
            'fulfillment_pct': 'mean',
            'image_allowed': 'max',
            'venue_disclosure': 'max'
        }).reset_index()

        grouped['score'] = (
            (1 / grouped['cost_per_verified_hh']) * 0.5 +
            grouped['fulfillment_pct'] * 0.3 +
            grouped['attendance_rate'] * 0.2
        ) * 100

        grouped = grouped.sort_values(by='score', ascending=False).head(4)

        results = []
        for _, row in grouped.iterrows():
            venue_events = df_filtered[df_filtered['venue'] == row['venue']]
            times = venue_events['event_time'].dropna().unique().tolist()
            top_times = best_times(times)

            results.append(VenueRecommendation(
                venue=row['venue'],
                city=city,
                state=state,
                most_recent_event=row['event_date'].strftime('%Y-%m-%d'),
                number_of_events=int(row['attended_hh']),
                avg_gross_registrants=round(row['gross_registrants'], 2),
                avg_cpa=round(row['cost_per_verified_hh'], 2),
                avg_cpr=round(row['fb_cpr'], 2),
                attendance_rate=round(row['attendance_rate'], 3),
                fulfillment_pct=round(row['fulfillment_pct'], 3),
                image_allowed=bool(row['image_allowed']),
                disclosure_needed=bool(row['venue_disclosure']),
                score=round(row['score'], 1),
                best_time_1=top_times[0] if len(top_times) > 0 else "N/A",
                best_time_2=top_times[1] if len(top_times) > 1 else "N/A"
            ))

        logger.info(f"Returning {len(results)} recommendations")
        return results

    except Exception as e:
        logger.exception("Error during report generation")
        raise HTTPException(status_code=500, detail=f"Error generating report: {str(e)}")









