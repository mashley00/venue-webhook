from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

try:
    df = pd.read_csv(CSV_URL, encoding='utf-8')
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
except Exception as e:
    raise RuntimeError(f"Failed to load or process dataset: {e}")

df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")

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
    topic_map = {"TIR": "taxes_in_retirement_567", "EP": "estate_planning_567", "SS": "social_security_567"}
    topic = topic_map.get(data.topic.upper())
    if not topic:
        raise HTTPException(status_code=400, detail="Invalid topic code.")

    filtered = df[
        (df['topic'].str.lower() == topic.lower()) &
        (df['city'].str.lower() == data.city.lower()) &
        (df['state'].str.lower() == data.state.lower())
    ]

    if filtered.empty:
        raise HTTPException(status_code=404, detail="No matching events found.")

    now = datetime.now()

    def calc_score(row):
        try:
            cpa = row['cost_per_verified_hh']
            max_reg = row['registration_max']
            fulfilled_pct = row['attended_hh'] / (max_reg / 2.4) if max_reg else 0
            attendance_rate = row['attended_hh'] / row['gross_registrants'] if row['gross_registrants'] else 0
            days_ago = (now - row['event_date']).days
            weight = 1.25 if days_ago <= 30 else (1.0 if days_ago <= 90 else 0.8)
            base_score = (1 / cpa * 0.5) + (fulfilled_pct * 0.3) + (attendance_rate * 0.2)
            return round(base_score * weight * 2.5, 2)
        except:
            return 0

    filtered["score"] = filtered.apply(calc_score, axis=1)

    grouped = filtered.groupby('venue').agg(
        city=('city', 'first'),
        state=('state', 'first'),
        most_recent_event=('event_date', 'max'),
        number_of_events=('venue', 'count'),
        avg_gross_registrants=('gross_registrants', 'mean'),
        avg_cpa=('cost_per_verified_hh', 'mean'),
        avg_cpr=('fb_cpr', 'mean'),
        attendance_rate=('attended_hh', lambda x: x.sum() / (filtered['gross_registrants'].sum() + 1e-6)),
        fulfillment_pct=('attended_hh', lambda x: x.sum() / (filtered['registration_max'].sum() / 2.4 + 1e-6)),
        image_allowed=('venue_image_allowed', lambda x: x.mode().iloc[0] if not x.empty else False),
        disclosure_needed=('venue_disclosure_needed', lambda x: x.mode().iloc[0] if not x.empty else False),
        score=('score', 'mean')
    ).reset_index()

    grouped = grouped.sort_values(by='score', ascending=False).head(4)

    def recommend_times(sub_df):
        times = sub_df['time'].dropna().value_counts().head(2).index.tolist()
        return times + ["N/A"] * (2 - len(times))

    results = []
    for _, row in grouped.iterrows():
        venue_df = filtered[filtered["venue"] == row["venue"]]
        times = recommend_times(venue_df)
        results.append(VORResponse(
            venue=row["venue"],
            city=row["city"],
            state=row["state"],
            most_recent_event=row["most_recent_event"].strftime("%Y-%m-%d"),
            number_of_events=row["number_of_events"],
            avg_gross_registrants=round(row["avg_gross_registrants"], 2),
            avg_cpa=round(row["avg_cpa"], 2),
            avg_cpr=round(row["avg_cpr"], 2),
            attendance_rate=round(row["attendance_rate"], 3),
            fulfillment_pct=round(row["fulfillment_pct"], 3),
            image_allowed=bool(row["image_allowed"]),
            disclosure_needed=bool(row["disclosure_needed"]),
            score=round(row["score"], 2),
            best_time_1=times[0],
            best_time_2=times[1]
        ))

    return results














