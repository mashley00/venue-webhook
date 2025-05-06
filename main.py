import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from datetime import datetime
from geopy.distance import geodesic
from geopy.geocoders import OpenCage
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY")
supabase: Client = create_client(SUPABASE_URL.strip(), SUPABASE_KEY.strip())

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
        response = supabase.table("all_events").select("*").execute()
        data = response.data
    except Exception:
        data = []

    # Fallback if Supabase is empty
    if not data:
        try:
            csv_url = "https://raw.githubusercontent.com/mashley00/VenueGPT/main/all_events_23_25.csv"
            df = pd.read_csv(csv_url, encoding='utf-8')
            data = df.to_dict(orient="records")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to load fallback CSV: {str(e)}")

    df = pd.DataFrame(data)
    if df.empty:
        return {"message": "No event data available."}

    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
    df = df[df["topic"].str.lower() == topic.lower()]
    if df.empty:
        return {"message": f"No events for topic '{topic}'"}

    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")

    # Geolocation
    geolocator = OpenCage(api_key=OPENCAGE_API_KEY)

    try:
        loc = geolocator.geocode(f"{request.city}, {request.state}")
        if not loc:
            return {"message": f"Could not geolocate {request.city}, {request.state}"}
        city_coords = (loc.latitude, loc.longitude)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Geolocation error: {str(e)}")

    def is_within_radius(row):
        try:
            venue_loc = geolocator.geocode(f"{row['venue']}, {row['city']}, {row['state']}")
            if venue_loc:
                return geodesic(city_coords, (venue_loc.latitude, venue_loc.longitude)).miles <= request.miles
        except:
            return False
        return False

    df = df[df.apply(is_within_radius, axis=1)]
    if df.empty:
        return {"message": "No venues found within search radius."}

    df["event_age_days"] = (datetime.now() - df["event_date"]).dt.days

    def score_row(row):
        try:
            cpa = row.get("cpa", 0)
            rate = row.get("attendance_rate", 0)
            fill = row.get("fulfillment_percent", 0)
            weight = 1.25 if row["event_age_days"] <= 30 else 1.0 if row["event_age_days"] <= 90 else 0.8
            return round(((1 / cpa if cpa else 0) * 0.5 + fill * 0.3 + rate * 0.2) * weight * 40, 2)
        except:
            return 0

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
        recent["dow"] = recent["event_date"].dt.day_name()
        recent["hour"] = pd.to_datetime(recent["event_time"], errors="coerce").dt.hour
        morning = recent[recent["hour"] == 11]
        evening = recent[recent["hour"] == 18]
        best_morning = morning["dow"].mode()[0] if not morning.empty else "Monday"
        best_evening = evening["dow"].mode()[0] if not evening.empty else "Tuesday"
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










