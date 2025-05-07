from fastapi import FastAPI, Request
from pydantic import BaseModel
from typing import List
import pandas as pd
import traceback
import os
import requests

app = FastAPI()

# === Models ===
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: int = 6

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
    image_allowed: bool
    disclosure_needed: bool
    score: float
    best_time_1: str
    best_time_2: str

# === Load Data ===
CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

def load_csv_data():
    print("Loading CSV data...")
    try:
        df = pd.read_csv(CSV_URL)
        print(f"CSV Loaded. Rows: {len(df)}. Columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        print("Error loading CSV:", e)
        raise

# === Route ===
@app.post("/vor", response_model=List[VenueResult])
async def get_venue_optimization_report(request: VORRequest):
    try:
        df = load_csv_data()

        # Step 1: Normalize & Filter by topic
        print(f"Filtering by topic: {request.topic}, city: {request.city}, state: {request.state}")
        topic_map = {
            "TIR": "Taxes in Retirement 567",
            "EP": "Estate Planning 567",
            "SS": "Social Security 567"
        }
        topic_name = topic_map.get(request.topic.upper())
        if not topic_name:
            raise ValueError(f"Invalid topic code: {request.topic}")

        df = df[df["Topic"] == topic_name]
        print(f"Filtered rows for topic: {len(df)}")

        df = df[
            (df["City"].str.lower() == request.city.lower()) &
            (df["State"].str.lower() == request.state.lower())
        ]
        print(f"Filtered rows for location: {len(df)}")

        # Placeholder scoring logic for now
        df["score"] = 10.0  # Temporary dummy

        result = []
        for _, row in df.iterrows():
            result.append(VenueResult(
                venue=row["Venue"],
                city=row["City"],
                state=row["State"],
                most_recent_event=str(row["Event Date"]),
                number_of_events=1,  # Replace later with grouped logic
                avg_gross_registrants=row.get("Gross Registrants", 0),
                avg_cpa=row.get("Cost per Verified HH", 0),
                avg_cpr=row.get("FB CPR", 0),
                attendance_rate=row.get("Attendance Rate", 0),
                fulfillment_pct=0.8,  # Placeholder
                image_allowed=row.get("Image Allowed", False) == True,
                disclosure_needed=row.get("Venue Disclosure Needed", False) == True,
                score=row["score"],
                best_time_1="6:00 PM on Monday",
                best_time_2="11:00 AM on Wednesday"
            ))

        print(f"Returning {len(result)} venues")
        return result

    except Exception as e:
        traceback_str = traceback.format_exc()
        print("Error in /vor endpoint:\n", traceback_str)
        return [{"error": str(e)}]  # Returns a list so response_model stays satisfied

















