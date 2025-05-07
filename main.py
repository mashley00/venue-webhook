from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
import os
import httpx

app = FastAPI()

S3_CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: Optional[int] = 6

class VenueResponse(BaseModel):
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

@app.post("/vor", response_model=List[VenueResponse])
async def get_venue_optimization_report(req: VORRequest):
    try:
        df = pd.read_csv(S3_CSV_URL)
        df.columns = [col.lower() for col in df.columns]  # Normalize headers

        topic = req.topic.lower()
        city = req.city.lower()
        state = req.state.lower()
        radius = req.miles

        # Filter dataset
        filtered = df[
            (df['city'].str.lower() == city) &
            (df['state'].str.lower() == state) &
            (df['topic'].str.lower() == topic)
        ]

        if filtered.empty:
            return []

        venue_stats = []
        for venue, group in filtered.groupby('venue'):
            most_recent_event = group['event date'].max()
            number_of_events = len(group)
            avg_gross_registrants = group['gross registrants'].mean()
            avg_cpa = group['cost per verified hh'].mean()
            avg_cpr = group['fb cpr'].mean()
            attendance_rate = (group['attended hh'] / group['gross registrants']).mean()
            fulfillment_pct = (group['attended hh'] / (group['registration max'] / 2.4)).mean()
            image_allowed = bool(group['image allowed'].iloc[0]) if 'image allowed' in group else False
            disclosure_needed = bool(group['venue disclosure'].iloc[0]) if 'venue disclosure' in group else False
            score = (1 / avg_cpa * 0.5) + (fulfillment_pct * 0.3) + (attendance_rate * 0.2)
            score = round(score * 40, 2)

            venue_stats.append({
                "venue": venue,
                "city": city.title(),
                "state": state.upper(),
                "most_recent_event": most_recent_event,
                "number_of_events": number_of_events,
                "avg_gross_registrants": round(avg_gross_registrants, 2),
                "avg_cpa": round(avg_cpa, 2),
                "avg_cpr": round(avg_cpr, 2),
                "attendance_rate": round(attendance_rate, 4),
                "fulfillment_pct": round(fulfillment_pct, 4),
                "image_allowed": image_allowed,
                "disclosure_needed": disclosure_needed,
                "score": score,
                "best_time_1": "11:00am Monday",
                "best_time_2": "6:00pm Monday"
            })

        venue_stats = sorted(venue_stats, key=lambda x: x["score"], reverse=True)
        return venue_stats[:4]

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=f"Processing error: {str(e)}")

















