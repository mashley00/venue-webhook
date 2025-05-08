from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import pandas as pd
import numpy as np
import requests
from io import StringIO

app = FastAPI()

VERSION = "2025.05.08.0137"
DATA_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: Optional[int] = 6

@app.get("/")
def root():
    return {"message": "Venue Optimization API", "version": VERSION}

@app.post("/vor")
def get_vor(request: VORRequest):
    try:
        # Load data
        response = requests.get(DATA_URL)
        if response.status_code != 200:
            return {"detail": f"Failed to load data: HTTP Error {response.status_code}"}        

        df = pd.read_csv(StringIO(response.text))

        # Basic cleaning
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')

        # Filter by inputs
        topic = request.topic.upper()
        city = request.city.strip().title()
        state = request.state.strip().upper()

        filtered = df[
            (df['topic'] == topic) &
            (df['city'].str.title() == city) &
            (df['state'].str.upper() == state)
        ].copy()

        if filtered.empty:
            return {"detail": "No matching records found."}

        # Calculate metrics
        filtered['attendance_rate'] = filtered['attended_hh'] / filtered['gross_registrants']
        filtered['fulfillment_pct'] = filtered['attended_hh'] / (filtered['registration_max'] / 2.4)
        filtered['cost_per_verified_hh'] = filtered['fb_cpr'] / filtered['attendance_rate']
        filtered['score'] = (
            (1 / filtered['cost_per_verified_hh'] * 0.5) +
            (filtered['fulfillment_pct'] * 0.3) +
            (filtered['attendance_rate'] * 0.2)
        ) * 100 / 40  # Convert to 100pt scale

        result = filtered[['venue', 'score']].sort_values(by='score', ascending=False).head(4)
        return result.to_dict(orient='records')

    except Exception as e:
        return {"detail": str(e)}



















