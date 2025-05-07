from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import List
import pandas as pd
import datetime
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

# Load the dataset from S3
CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
try:
    df = pd.read_csv(CSV_URL, encoding='utf-8')
except Exception as e:
    raise RuntimeError(f"Failed to load dataset: {e}")

# Convert date column
df['Event Date'] = pd.to_datetime(df['Event Date'], errors='coerce')

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
def get_vor(data: VORRequest):
    topic_map = {"TIR": "Taxes in Retirement 567", "EP": "Estate Planning 567", "SS": "Social Security 567"}
    topic = topic_map.get(data.topic.upper())

    if not topic:
        raise HTTPException(status_code=400, detail="Invalid topic code.")

    # Filter by topic, city, state
    filtered = df[
        (df['Topic'].str.strip().str.lower() == topic.strip().lower()) &
        (df['City'].str.strip().str.lower() == data.city.strip().lower()) &
        (df['State'].str.strip().str.lower() == data.state.strip().lower())
    ]

    if filtered.empty:
        raise HTTPException(status_code=404, detail="No matching events found.")

    now = pd.Timestamp.now()

    def calc_score(row):
        try:
            cpa = row['Cost per Verified HH']
            max_reg = row['Registration Max']
            fulfilled_pct = row['Attended HH'] / (max_reg / 2.4) if max_reg and max_reg > 0 else 0
            attendance_rate = row['Attended HH'] / row['Gross Registrants'] if row['Gross Registrants'] > 0 else 0
            weight = 1.0
            days_ago = (now - row['Event Date']).days
            if days_ago <= 30:
                weight = 1.25
            elif 31 <= days_ago <= 90:
                weight = 1.0
            else:
                weight = 0.8
            base_score = (1 / cpa * 0.5) + (fulfilled_pct * 0.3) + (attendance_rate * 0.2)
            return round(base_score * weight * 2.5, 2)  # Scale to 100
        except:
            return 0

    filtered = filtered.copy()
    filtered['score'] = filtered.apply(calc_score, axis=1)

    grouped = filtered.groupby('Venue').agg(
        city=('City', 'first'),
        state=('State', 'first'),
        most_recent_event=('Event Date', 'max'),
        number_of_events=('Venue', 'count'),
        avg_gross_registrants=('Gross Registrants', 'mean'),
        avg_cpa=('Cost per Verified HH', 'mean'),
        avg_cpr=('FB CPR', 'mean'),
        attendance_rate=('Attended HH', lambda x: x.sum()) / (filtered['Gross Registrants'].sum() + 1e-6),
        fulfillment_pct=('Attended HH', lambda x: x.sum() / (filtered['Registration Max'].sum() / 2.4 + 1e-6)),
        image_allowed=('Venue Image Allowed', lambda x: x.mode().iloc[0] if not x.empty else False),
        disclosure_needed=('Venue Disclosure', lambda x: x.mode().iloc[0] if not x.empty else False),
        score=('score', 'mean')
    ).reset_index()

    grouped = grouped.sort_values(by='score', ascending=False).head(4)

    def recommend_times(venue_df):
        best = venue_df[venue_df['Venue'] == venue_df['Venue'].iloc[0]]
        times = best['Time'].dropna().value_counts()
        days = best['Event Date'].dt.day_name().value_counts()
        best_times = times.head(2).index.tolist()
        best_days = days.head(2).index.tolist()
        return best_times, best_days

    results = []
    for _, row in grouped.iterrows():
        venue_name = row['Venue']
        venue_data = filtered[filtered['Venue'] == venue_name]
        times, days = recommend_times(venue_data)
        results.append(VORResponse(
            venue=venue_name,
            city=row['city'],
            state=row['state'],
            most_recent_event=row['most_recent_event'].strftime('%Y-%m-%d') if pd.notnull(row['most_recent_event']) else '',
            number_of_events=int(row['number_of_events']),
            avg_gross_registrants=round(row['avg_gross_registrants'], 2),
            avg_cpa=round(row['avg_cpa'], 2),
            avg_cpr=round(row['avg_cpr'], 2),
            attendance_rate=round(row['attendance_rate'], 3),
            fulfillment_pct=round(row['fulfillment_pct'], 3),
            image_allowed=bool(row['image_allowed']) if pd.notnull(row['image_allowed']) else False,
            disclosure_needed=bool(row['disclosure_needed']) if pd.notnull(row['disclosure_needed']) else False,
            score=round(row['score'], 2),
            best_time_1=times[0] if times else "N/A",
            best_time_2=times[1] if len(times) > 1 else "N/A"
        ))

    return results













