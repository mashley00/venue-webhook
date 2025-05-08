# Dataset loaded from S3 on 2025-05-08

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import requests

app = FastAPI()

# Allow CORS for testing tools like Postman or frontend clients
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load dataset from S3 at startup
S3_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

try:
    df = pd.read_csv(S3_URL)
    df['Event Date'] = pd.to_datetime(df['Event Date'], errors='coerce')
except Exception as e:
    print("Error loading CSV:", e)
    df = pd.DataFrame()  # fallback

# Response model for venue optimization
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

@app.post("/vor", response_model=List[VenueOption])
def get_vor(
    topic: str = Query(..., description="Seminar topic (TIR, EP, SS)"),
    city: str = Query(...),
    state: str = Query(...),
    miles: int = Query(6)
):
    try:
        # Topic translation
        topic_map = {"TIR": "Taxes in Retirement 567", "EP": "Estate Planning 567", "SS": "Social Security 567"}
        topic_full = topic_map.get(topic.upper())
        if topic_full is None:
            return [{"error": "Invalid topic"}]

        # Filter by topic and location
        filtered = df[
            (df["Topic"] == topic_full) &
            (df["City"].str.lower() == city.lower()) &
            (df["State"].str.lower() == state.lower())
        ].copy()

        if filtered.empty:
            return []

        # Calculated metrics
        filtered["attendance_rate"] = filtered["attended_hh"] / filtered["gross_registrants"]
        filtered["fulfillment_pct"] = filtered["attended_hh"] / (filtered["registration_max"] / 2.4)
        filtered["cost_per_verified_hh"] = filtered["fb_cpr"] / filtered["attendance_rate"]

        # Grouping and scoring
        grouped = filtered.groupby("Venue").agg({
            "City": "first",
            "State": "first",
            "Event Date": "max",
            "gross_registrants": "mean",
            "cost_per_verified_hh": "mean",
            "fb_cpr": "mean",
            "attendance_rate": "mean",
            "fulfillment_pct": "mean",
            "Image Allowed": "first",
            "Disclosure Needed": "first",
            "Time": lambda x: x.value_counts().index[0],
            "Day": lambda x: x.value_counts().index[0],
        }).reset_index()

        # Compute score
        grouped["score"] = (
            (1 / grouped["cost_per_verified_hh"]) * 50 +
            grouped["fulfillment_pct"] * 30 +
            grouped["attendance_rate"] * 20
        )

        results = []
        for _, row in grouped.iterrows():
            results.append(VenueOption(
                venue=row["Venue"],
                city=row["City"],
                state=row["State"],
                most_recent_event=row["Event Date"].strftime("%Y-%m-%d") if pd.notnull(row["Event Date"]) else "N/A",
                number_of_events=filtered[filtered["Venue"] == row["Venue"]].shape[0],
                avg_gross_registrants=round(row["gross_registrants"], 2),
                avg_cpa=round(row["cost_per_verified_hh"], 2),
                avg_cpr=round(row["fb_cpr"], 2),
                attendance_rate=round(row["attendance_rate"], 2),
                fulfillment_pct=round(row["fulfillment_pct"], 2),
                image_allowed=row["Image Allowed"] == "Y",
                disclosure_needed=row["Disclosure Needed"] == "Y",
                score=round(row["score"], 2),
                best_time_1=f"{row['Time']} on {row['Day']}",
                best_time_2="TBD"
            ))

        return sorted(results, key=lambda x: x.score, reverse=True)

    except Exception as e:
        print("ERROR IN /vor:", e)
        return [{"error": str(e)}]


















