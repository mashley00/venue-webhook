from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
import requests

app = FastAPI()

# Define your response model
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

@app.post("/vor", response_model=List[VenueRecommendation])
async def get_venue_recommendations(request: Request):
    try:
        body = await request.json()
        topic = body.get("topic")
        city = body.get("city")
        state = body.get("state")
        miles = body.get("miles", 6)

        # Debugging input
        print("Received Request:", body)

        # Load data from S3
        url = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
        df = pd.read_csv(url)

        # Debug the shape and column names
        print("Data loaded successfully. Columns:", df.columns.tolist())

        # Filter based on input
        df_filtered = df[
            (df["Topic"].str.upper() == topic.upper()) &
            (df["City"].str.lower() == city.lower()) &
            (df["State"].str.upper() == state.upper())
        ]

        # Debug filtered data
        print("Filtered DataFrame Shape:", df_filtered.shape)
        print("Filtered Sample:", df_filtered.head().to_dict(orient="records"))

        if df_filtered.empty:
            raise ValueError(f"No data found for Topic={topic}, City={city}, State={state}")

        # Simplified scoring & dummy values for now (fill in real logic here)
        results = []
        for venue_name in df_filtered["Venue"].unique():
            venue_df = df_filtered[df_filtered["Venue"] == venue_name]
            results.append({
                "venue": venue_name,
                "city": city,
                "state": state,
                "most_recent_event": venue_df["Event Date"].max(),
                "number_of_events": len(venue_df),
                "avg_gross_registrants": round(venue_df["Gross Registrants"].mean(), 2),
                "avg_cpa": round(venue_df["Cost per Verified HH"].mean(), 2),
                "avg_cpr": round(venue_df["FB CPR"].mean(), 2),
                "attendance_rate": round((venue_df["Attended HH"].sum() / venue_df["Gross Registrants"].sum()), 2),
                "fulfillment_pct": round((venue_df["Attended HH"].sum() / (venue_df["Registration Max"].sum() / 2.4)), 2),
                "image_allowed": venue_df["Image Allowed"].iloc[0] == "Y",
                "disclosure_needed": venue_df["Disclosure"].iloc[0] == "Y",
                "score": 30.0,  # placeholder
                "best_time_1": "11:00am Monday",
                "best_time_2": "6:00pm Monday"
            })

        return results

    except Exception as e:
        print("❌ ERROR in /vor:", str(e))
        return [{"error": str(e)}]















