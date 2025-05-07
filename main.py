from fastapi import FastAPI, Query
from pydantic import BaseModel
import pandas as pd
import requests
from datetime import datetime
from typing import List, Optional
import io

app = FastAPI()

class VORRequest(BaseModel):
    topic: str  # "TIR", "EP", "SS"
    city: str
    state: str
    miles: Optional[float] = 6.0

class VenueResult(BaseModel):
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
    image_allowed: str
    disclosure_needed: str
    score: float
    best_time_1: str
    best_time_2: str
    flags: List[str] = []

@app.post("/vor", response_model=List[VenueResult])
def run_vor(request: VORRequest):
    try:
        # Load from S3
        s3_url = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
        response = requests.get(s3_url)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text), encoding='utf-8')

        # Filter relevant data
        df = df[df['Topic'] == request.topic]
        df = df[(df['City'].str.lower() == request.city.lower()) & (df['State'].str.lower() == request.state.lower())]

        if df.empty:
            return []

        # Compute metrics
        df['Attended HH'] = pd.to_numeric(df['Attended HH'], errors='coerce')
        df['Gross Registrants'] = pd.to_numeric(df['Gross Registrants'], errors='coerce')
        df['FB CPR'] = pd.to_numeric(df['FB CPR'], errors='coerce')
        df['Cost per Verified HH'] = df['FB CPR'] / (df['Attended HH'] / df['Gross Registrants'])

        df['Attendance Rate'] = df['Attended HH'] / df['Gross Registrants']
        df['Fulfillment %'] = df['Attended HH'] / (df['Registration Max'] / 2.4)
        df['Event Date'] = pd.to_datetime(df['Event Date'], errors='coerce')

        today = datetime.today()
        df['Recency Weight'] = df['Event Date'].apply(
            lambda x: 1.25 if (today - x).days <= 30 else (1.0 if (today - x).days <= 90 else 0.8)
        )

        df['Score'] = ((1 / df['Cost per Verified HH']) * 0.5 + df['Fulfillment %'] * 0.3 + df['Attendance Rate'] * 0.2) * df['Recency Weight'] * 40

        venues = df.groupby('Venue').agg({
            'City': 'first',
            'State': 'first',
            'Event Date': 'max',
            'Gross Registrants': 'mean',
            'Cost per Verified HH': 'mean',
            'FB CPR': 'mean',
            'Attendance Rate': 'mean',
            'Fulfillment %': 'mean',
            'Venue Image Allowed': 'first',
            'Venue Disclosure Needed': 'first',
            'Score': 'mean'
        }).reset_index()

        # Sort and prepare response
        venues = venues.sort_values(by='Score', ascending=False).head(4)

        result = []
        for _, row in venues.iterrows():
            flags = []
            if row['Venue Image Allowed'] == 'No':
                flags.append("Image Not Allowed")
            if row['Venue Disclosure Needed'] == 'Yes':
                flags.append("Disclosure Required")

            result.append(VenueResult(
                venue=row['Venue'],
                city=row['City'],
                state=row['State'],
                most_recent_event=row['Event Date'].strftime("%Y-%m-%d"),
                number_of_events=int(df[df['Venue'] == row['Venue']].shape[0]),
                avg_gross_registrants=round(row['Gross Registrants'], 1),
                avg_cpa=round(row['Cost per Verified HH'], 2),
                avg_cpr=round(row['FB CPR'], 2),
                attendance_rate=round(row['Attendance Rate'], 2),
                fulfillment_pct=round(row['Fulfillment %'], 2),
                image_allowed="✅" if row['Venue Image Allowed'] == 'Yes' else "❌",
                disclosure_needed="✅" if row['Venue Disclosure Needed'] == 'Yes' else "❌",
                score=round(row['Score'], 1),
                best_time_1="11:00am on Monday",
                best_time_2="6:00pm on Monday",
                flags=flags
            ))

        return result
    except Exception as e:
        return [{"error": str(e)}]

















