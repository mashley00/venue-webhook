from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
import traceback
import os

app = FastAPI()

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic response model
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

@app.get("/vor", response_model=List[VenueRecommendation])
async def get_venue_report(
    topic: str = Query(..., description="Seminar topic code: TIR, EP, or SS"),
    city: str = Query(..., description="City"),
    state: str = Query(..., description="State"),
    miles: Optional[float] = Query(6.0, description="Search radius in miles")
):
    try:
        # Load CSV from S3
        s3_url = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
        df = pd.read_csv(s3_url)

        # Verify columns
        required_columns = [
            "Topic", "City", "State", "Venue", "Event Date", "Job Number",
            "Gross Registrants", "Attended HH", "FB CPR", "Cost per Verified HH",
            "Venue Image Allowed", "Venue Disclosure", "Time", "Day"
        ]
        for col in required_columns:
            if col not in df.columns:
                return [{"error": f"Missing required column: {col}"}]

        # Apply topic + location filters
        df = df[
            (df['Topic'] == topic.upper()) &
            (df['City'].str.lower() == city.lower()) &
            (df['State'].str.lower() == state.lower())
        ].copy()

        if df.empty:
            return []

        # Data cleanup and calculations
        df["Gross Registrants"] = pd.to_numeric(df["Gross Registrants"], errors="coerce")
        df["Attended HH"] = pd.to_numeric(df["Attended HH"], errors="coerce")
        df["FB CPR"] = pd.to_numeric(df["FB CPR"], errors="coerce")
        df["Cost per Verified HH"] = pd.to_numeric(df["Cost per Verified HH"], errors="coerce")

        df["Attendance Rate"] = df["Attended HH"] / df["Gross Registrants"]
        df["Fulfillment %"] = df["Attended HH"] / (df["Gross Registrants"] / 2.4)

        df_grouped = df.groupby("Venue").agg({
            "City": "first",
            "State": "first",
            "Event Date": "max",
            "Gross Registrants": "mean",
            "Cost per Verified HH": "mean",
            "FB CPR": "mean",
            "Attendance Rate": "mean",
            "Fulfillment %": "mean",
            "Venue Image Allowed": "first",
            "Venue Disclosure": "first"
        }).reset_index()

        # Score calculation
        df_grouped["Score"] = (
            (1 / df_grouped["Cost per Verified HH"]) * 0.5 +
            df_grouped["Fulfillment %"] * 0.3 +
            df_grouped["Attendance Rate"] * 0.2
        ) * 40  # Scale to 40

        # Sort and trim to top 4
        top_venues = df_grouped.sort_values(by="Score", ascending=False).head(4)

        # Assign best times (static fallback for now)
        best_times = ["11:00am on Monday", "6:00pm on Monday"]

        # Prepare structured response
        response = []
        for _, row in top_venues.iterrows():
            response.append({
                "venue": row["Venue"],
                "city": row["City"],
                "state": row["State"],
                "most_recent_event": row["Event Date"],
                "number_of_events": int(df[df["Venue"] == row["Venue"]].shape[0]),
                "avg_gross_registrants": round(row["Gross Registrants"], 2),
                "avg_cpa": round(row["Cost per Verified HH"], 2),
                "avg_cpr": round(row["FB CPR"], 2),
                "attendance_rate": round(row["Attendance Rate"], 2),
                "fulfillment_pct": round(row["Fulfillment %"], 2),
                "image_allowed": row["Venue Image Allowed"] == "TRUE",
                "disclosure_needed": row["Venue Disclosure"] == "TRUE",
                "score": round(row["Score"], 2),
                "best_time_1": best_times[0],
                "best_time_2": best_times[1]
            })

        return response

    except Exception as e:
        print("Error occurred:", traceback.format_exc())
        return [{"error": str(e)}]















