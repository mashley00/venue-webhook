import pandas as pd
import numpy as np
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import os

from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: int = 6

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
async def get_vor(request: VORRequest):
    try:
        df_url = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
        df = pd.read_csv(df_url, encoding="utf-8")

        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')

        topic_map = {"TIR": "taxes_in_retirement_567", "EP": "estate_planning_567", "SS": "social_security_567"}
        topic_mapped = topic_map.get(request.topic.upper())
        if not topic_mapped:
            raise HTTPException(status_code=400, detail="Invalid topic code.")

        df = df[df['topic'].str.lower() == topic_mapped.lower()]
        df = df[df['city'].str.lower() == request.city.lower()]
        df = df[df['state'].str.lower() == request.state.lower()]

        # Compute derived metrics
        filtered = df.copy()
        filtered['attendance_rate'] = filtered['attended_hh'] / filtered['gross_registrants']
        filtered['fulfillment_pct'] = filtered['attended_hh'] / (filtered['registration_max'] / 2.4)
        filtered['cost_per_verified_hh'] = filtered['fb_cpr'] / filtered['attendance_rate']
        filtered = filtered.replace([np.inf, -np.inf], np.nan).dropna(subset=['attendance_rate', 'cost_per_verified_hh'])

        # Group and average
        grouped = filtered.groupby('venue').agg({
            'city': 'first',
            'state': 'first',
            'event_date': 'max',
            'gross_registrants': 'mean',
            'cost_per_verified_hh': 'mean',
            'fb_cpr': 'mean',
            'attendance_rate': 'mean',
            'fulfillment_pct': 'mean',
            'image_allowed': 'first',
            'disclosure_needed': 'first',
            'job_number': 'count'
        }).reset_index()

        # Compute score on 100-point scale
        grouped['score'] = (
            (1 / grouped['cost_per_verified_hh']) * 0.5 +
            grouped['fulfillment_pct'] * 0.3 +
            grouped['attendance_rate'] * 0.2
        ) * 100

        # Sort and select top 4
        top_venues = grouped.sort_values(by='score', ascending=False).head(4)

        # Build response list
        response = []
        for _, row in top_venues.iterrows():
            response.append(VORResponse(
                venue=row['venue'],
                city=row['city'],
                state=row['state'],
                most_recent_event=row['event_date'].strftime('%Y-%m-%d') if pd.notnull(row['event_date']) else "N/A",
                number_of_events=int(row['job_number']),
                avg_gross_registrants=round(row['gross_registrants'], 2),
                avg_cpa=round(row['cost_per_verified_hh'], 2),
                avg_cpr=round(row['fb_cpr'], 2),
                attendance_rate=round(row['attendance_rate'], 2),
                fulfillment_pct=round(row['fulfillment_pct'], 2),
                image_allowed=bool(row['image_allowed']),
                disclosure_needed=bool(row['disclosure_needed']),
                score=round(row['score'], 2),
                best_time_1="11:00 AM on Monday",
                best_time_2="6:00 PM on Monday"
            ))
        return response

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

















