import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from datetime import datetime
from geopy.distance import geodesic
from dotenv import load_dotenv
from geopy.geocoders import Nominatim

# === Load environment ===
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise Exception("Missing Supabase environment variables.")
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

@app.get("/debug")
def debug():
    return {"message": "main.py is active and deployed"}

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
    topic_key = topic_map.get(request.topic.upper())
    if not topic_key:
        raise HTTPException(status_code=400, detail="Invalid topic code.")

    try:
        data = supabase.table("all_events").select("*").execute().data
        df = pd.DataFrame(data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Supabase fetch error: {str(e)}")

    if df.empty:
        return {"message": "No events data found."}

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
    df = df[df["topic"].str.lower() == topic_key.lower()]

    if df.empty:
        return {"message": f"No events found for topic: {request.topic}"}

    for col in ["event_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    city_coords = None
    try:
        geolocator = Nominatim(user_agent="vor_locator")
        city_location = geolocator.geocode(f"{request.city}, {request.state}")
        if not city_location:
            raise ValueError()
        city_coords = (city_location.latitude, city_location.longitude)
    except:
        return {"error": f"Unable to locate city: {request.city}, {request.state}"}

    def is_within_radius(row):
        try:
            venue_loc = geolocator.geocode(f"{row['venue']}, {row['city']}, {row['state']}")
            if not venue_loc:
                return False
            venue_coords = (venue_loc.latitude, venue_loc.longitude)
            return geodesic(city_coords, venue_coords).miles <= request.miles
        except:
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




