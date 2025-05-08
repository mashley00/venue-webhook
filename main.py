import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import datetime

# ---- Constants ----
S3_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
VERSION = "VenueGPT v1.0.1 - 2025-05-08"

# ---- FastAPI App ----
app = FastAPI(title="Venue Optimization Report", version=VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Models ----
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: Optional[float] = 6.0

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

# ---- Utility Functions ----
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
    if len(filtered) >= 2:
        return filtered[:2]
    return (filtered + times[:2])[:2]

# ---- Load Data ----
try:
    df = pd.read_csv(S3_URL)
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
except Exception as e:
    raise RuntimeError(f"Error loading or processing data from S3: {e}")

# ---- Endpoint ----
@app.post("/vor", response_model=List[VenueRecommendation])
def get_vor(request: VORRequest):
    try:
        topic = request.topic
        city = request.city
        state = request.state
        miles = request.miles

        reference_date = datetime.datetime.now()
        topic_map = {"TIR": "taxes_in_retirement_567", "EP": "estate_planning_567", "SS": "social_security_567"}
        if topic not in topic_map:
            raise HTTPException(status_code=400, detail="Invalid topic. Must be one of: TIR, EP, SS")

        df_filtered = df[
            (df['topic'].str.lower() == topic_map[topic].lower()) &
            (df['city'].str.lower() == city.lower()) &
            (df['state'].str.lower() == state.lower()) &
            (df['event_date'].notnull())
        ].copy()

        if df_filtered.empty:
            raise HTTPException(status_code=404, detail="No matching venue data found.")

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

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating report: {str(e)}")








