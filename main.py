# main.py (FULL COPY)

import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from datetime import datetime
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from dotenv import load_dotenv

# === Load environment ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL.strip(), SUPABASE_KEY.strip())

# === App setup ===
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class VorRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: float = 6.0

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/preview")
def preview_data(limit: int = 5):
    try:
        response = supabase.table("all_events").select("*").limit(limit).execute()
        return response.data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/vor")
def venue_optimization(request: VorRequest):
    topic_map = {
        "TIR": "taxes_in_retirement_567",
        "EP": "estate_planning_567",
        "SS": "social_security_567"
    }

    topic = topic_map.get(request.topic.upper(), request.topic.lower())
    try:
        data = supabase.table("all_events").select("*").execute().data
        df = pd.DataFrame(data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Supabase error: {str(e)}")

    if df.empty:
        return {"message": "No events found in Supabase."}

    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
    df = df[df["topic"].str.lower() == topic.lower()]
    if df.empty:
        return {"message": f"No data found for topic: {topic}"}

    for col in ["event_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    geolocator = Nominatim(user_agent="vor_locator")
    loc = geolocator.geocode(f"{request.city}, {request.state}")
    if not loc:
        return {"error": f"Could not locate {request.city}, {request.state}"}
    city_coords = (loc.latitude, loc.longitude)

    def is_within_radius(row):
        try:
            venue_loc = geolocator.geocode(f"{row['venue']}, {row['city']}, {row['state']}")
            if venue_loc:
                venue_coords = (venue_loc.latitude, venue_loc.longitude)
                return geodesic(city_coords, venue_coords).miles <= request.miles
        except:
            return False
        return False

    df = df[df.apply(is_within_radius, axis=1)]
    if df.empty:
        return {"message": f"No venues found within {request.miles} miles of {request.city}, {request.state}"}

    df["event_age_days"] = (datetime.now() - df["event_date"]).dt.days

    def score_row(row):
        cpa = row.get("cpa", 0)
        rate = row.get("attendance_rate", 0)
        fill = row.get("fulfillment_percent", 0)
        weight = 1.25 if row["event_age_days"] <= 30 else 1.0 if row["event_age_days"] <= 90 else 0.8
        score = ((1 / cpa if cpa else 0) * 0.5 + fill * 0.3 + rate * 0.2) * weight * 40
        return round(score, 2)

    df["score"] = df.apply(score_row, axis=1)

    grouped = (
        df.groupby("venue")
        .agg({
            "event_date": "max",
            "job_number": "count",
            "gross_registrants": "mean",
            "cpa": "mean",
            "fb_cpr": "mean",
            "attendance_rate": "mean",
            "fulfillment_percent": "mean",
            "venue_image_allowed": "last",
            "venue_disclosure_needed": "last",
            "score": "mean"
        })
        .reset_index()
        .sort_values(by="score", ascending=False)
        .head(4)
    )

    def suggest_times(venue):
        recent = df[df["venue"] == venue].copy()
        if recent.empty:
            return ["6:00 PM on Monday", "11:00 AM on Tuesday"]
        recent["dow"] = recent["event_date"].dt.day_name()
        recent["hour"] = pd.to_datetime(recent["event_time"], errors="coerce").dt.hour
        morning = recent[recent["hour"] == 11]
        evening = recent[recent["hour"] == 18]
        best_morning = morning["dow"].mode()[0] if not morning.empty else "Monday"
        best_evening = evening["dow"].mode()[0] if not evening.empty else "Monday"
        return [f"11:00 AM on {best_morning}", f"6:00 PM on {best_evening}"]

    results = []
    for _, row in grouped.iterrows():
        results.append({
            "🏆 Venue": row["venue"],
            "📍 Location": f"{request.city.title()}, {request.state.upper()}",
            "📅 Most Recent Event": row["event_date"].strftime("%Y-%m-%d"),
            "🗓️ Number of Events": int(row["job_number"]),
            "📈 Avg. Gross Registrants": round(row["gross_registrants"]),
            "💰 Avg. CPA": round(row["cpa"], 2),
            "💵 Avg. CPR": round(row["fb_cpr"], 2),
            "📊 Attendance Rate": f"{round(row['attendance_rate'] * 100, 1)}%",
            "🎯 Fulfillment %": f"{round(row['fulfillment_percent'] * 100, 1)}%",
            "📸 Image Allowed": "✅" if row["venue_image_allowed"] else "❌",
            "⚠️ Disclosure Needed": "✅" if row["venue_disclosure_needed"] else "❌",
            "🥇 Score": f"{round(row['score'], 2)} / 40",
            "⏰ Best Times": suggest_times(row["venue"])
        })

    return {"results": results}





