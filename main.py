import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime

app = FastAPI()

# ✅ Correct CSV URL
DATA_URL = "https://raw.githubusercontent.com/mashley00/VenueGPT/main/All%20Events%2023%20to%2025%20TIR%20EP%20SS%20CSV%20UTF%208.csv"

# 📦 Pydantic request model
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: int = 6

# 📦 Pydantic response model
class VenueOption(BaseModel):
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

def normalize_topic(topic: str) -> str:
    mapping = {
        "TIR": "Taxes in Retirement 567",
        "EP": "Estate Planning 567",
        "SS": "Social Security 567"
    }
    return mapping.get(topic.upper(), topic)

@app.post("/vor", response_model=List[VenueOption])
def get_vor(request: VORRequest):
    try:
        df = pd.read_csv(DATA_URL)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load data: {e}")

    topic_name = normalize_topic(request.topic)
    df['Event Date'] = pd.to_datetime(df['Event Date'], errors='coerce')
    df = df[df['Event Date'].notna()]
    today = pd.Timestamp.today()

    # 🧹 Filters
    filtered = df[
        (df['Topic'] == topic_name) &
        (df['City'].str.lower() == request.city.lower()) &
        (df['State'].str.lower() == request.state.lower())
    ].copy()

    if filtered.empty:
        raise HTTPException(status_code=404, detail="No events found for that location and topic.")

    # 📊 Calculations
    filtered['attendance_rate'] = filtered['Attended HH'] / filtered['Gross Registrants']
    filtered['fulfillment_pct'] = filtered['Attended HH'] / (filtered['Registration Max'] / 2.4)
    filtered['cost_per_verified_hh'] = filtered['FB CPR'] / filtered['attendance_rate']

    def calculate_score(row, days_old):
        base_score = (1 / row['cost_per_verified_hh']) * 0.5 + \
                     row['fulfillment_pct'] * 0.3 + \
                     row['attendance_rate'] * 0.2
        weight = 1.0
        if days_old <= 30:
            weight = 1.25
        elif days_old > 90:
            weight = 0.8
        return base_score * weight * 25  # ⬅️ 100 pt scale

    filtered['days_old'] = (today - filtered['Event Date']).dt.days
    filtered['score'] = filtered.apply(lambda row: calculate_score(row, row['days_old']), axis=1)

    grouped = filtered.groupby('Venue').agg(
        city=('City', 'first'),
        state=('State', 'first'),
        most_recent_event=('Event Date', 'max'),
        number_of_events=('Venue', 'count'),
        avg_gross_registrants=('Gross Registrants', 'mean'),
        avg_cpa=('cost_per_verified_hh', 'mean'),
        avg_cpr=('FB CPR', 'mean'),
        attendance_rate=('attendance_rate', 'mean'),
        fulfillment_pct=('fulfillment_pct', 'mean'),
        image_allowed=('Venue Image Allowed', lambda x: x.mode()[0] if not x.mode().empty else False),
        disclosure_needed=('Venue Disclosure Needed', lambda x: x.mode()[0] if not x.mode().empty else False),
        score=('score', 'mean')
    ).reset_index()

    # 📅 Assign best times — static fallback
    grouped['best_time_1'] = "11:00 AM Monday"
    grouped['best_time_2'] = "6:00 PM Monday"

    sorted_grouped = grouped.sort_values(by='score', ascending=False).head(4)

    results = [
        VenueOption(
            venue=row['Venue'],
            city=row['city'],
            state=row['state'],
            most_recent_event=row['most_recent_event'].strftime("%Y-%m-%d"),
            number_of_events=int(row['number_of_events']),
            avg_gross_registrants=round(row['avg_gross_registrants'], 2),
            avg_cpa=round(row['avg_cpa'], 2),
            avg_cpr=round(row['avg_cpr'], 2),
            attendance_rate=round(row['attendance_rate'], 2),
            fulfillment_pct=round(row['fulfillment_pct'], 2),
            image_allowed=bool(row['image_allowed']),
            disclosure_needed=bool(row['disclosure_needed']),
            score=round(row['score'], 2),
            best_time_1=row['best_time_1'],
            best_time_2=row['best_time_2']
        )
        for _, row in sorted_grouped.iterrows()
    ]

    return results

















