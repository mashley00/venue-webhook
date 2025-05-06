import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from datetime import datetime
from geopy.distance import geodesic
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print(f"✅ SUPABASE_URL raw: '{SUPABASE_URL}'")
print(f"✅ SUPABASE_KEY raw: '{SUPABASE_KEY[:10]}...'")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

df = pd.DataFrame()

def clean_columns(df):
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower().str.replace(r"[^\w\s]", "", regex=True)
    return df

def load_and_prepare_data():
    print("✅ Loading and preparing data from Supabase...")
    response = supabase.table("all_events").select("*").execute()

    if not response.data:
        raise Exception("⚠️ No data returned from Supabase.")

    global df
    df = pd.DataFrame(response.data)

    if df.empty:
        raise Exception("⚠️ Dataframe is empty after Supabase load.")

    df = clean_columns(df)
    date_cols = ["event_date", "marketing_start_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    numeric_cols = [
        "registration_max", "fulfillment_percent", "net_registrants", "gross_registrants",
        "fb_registrants", "fb_cpr", "attended_hh", "walk_ins", "attendance_rate", "cpa",
        "fb_days_running", "fb_impressions", "cpm", "fb_reach"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"✅ Loaded {len(df)} rows with columns: {list(df.columns)}")

@app.on_event("startup")
def startup_event():
    load_and_prepare_data()

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/preview")
def preview_data(limit: int = 10):
    return df.head(limit).to_dict(orient="records")

class VorRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: float = 6.0

@app.post("/vor")
def venue_optimization(request: VorRequest):
    topic = request.topic.strip().lower()
    city = request.city.strip().lower()
    state = request.state.strip().lower()
    miles = request.miles

    filtered = df.copy()

    # Get lat/lon for central city
    try:
        from geopy.geocoders import Nominatim
        geolocator = Nominatim(user_agent="vor_locator")
        location = geolocator.geocode(f"{city}, {state}")
        if location is None:
            return {"error": f"Could not locate {city}, {state}"}
        city_coords = (location.latitude, location.longitude)
    except:
        return {"error": "Failed to geolocate city"}

    def is_within_radius(row):
        venue_coords = geolocator.geocode(f"{row['venue']}, {row['city']}, {row['state']}")
        if venue_coords is None:
            return False
        venue_coords = (venue_coords.latitude, venue_coords.longitude)
        return geodesic(city_coords, venue_coords).miles <= miles

    filtered = filtered[filtered["topic"].str.lower() == topic]
    filtered = filtered[filtered["state"].str.lower() == state]
    filtered = filtered[filtered["city"].str.lower() == city]

    if filtered.empty:
        return {"message": f"No data available for {topic.upper()} in {city.title()}, {state.upper()}"}

    filtered["event_age_days"] = (datetime.now() - filtered["event_date"]).dt.days
    def score_row(row):
        cpa = row.get("cpa", 0)
        att_rate = row.get("attendance_rate", 0)
        fulfilled = row.get("fulfillment_percent", 0)
        multiplier = 1.0
        if row["event_age_days"] <= 30:
            multiplier = 1.25
        elif row["event_age_days"] <= 90:
            multiplier = 1.0
        else:
            multiplier = 0.8
        base_score = ((1 / cpa if cpa else 0) * 0.5) + (fulfilled * 0.3) + (att_rate * 0.2)
        return round(base_score * multiplier * 40, 2)

    filtered["score"] = filtered.apply(score_row, axis=1)
    grouped = (
        filtered.groupby("venue")
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
        recent = filtered[filtered["venue"] == venue].copy()
        if recent.empty:
            return ["6:00 PM on Monday", "11:00 AM on Tuesday"]
        recent["dow"] = recent["event_date"].dt.day_name()
        recent["hour"] = pd.to_datetime(recent["event_time"], errors="coerce").dt.hour
        morning_slots = recent[recent["hour"].isin([11])]
        evening_slots = recent[recent["hour"].isin([18])]
        best_morning_day = morning_slots["dow"].mode()[0] if not morning_slots.empty else "Monday"
        best_evening_day = evening_slots["dow"].mode()[0] if not evening_slots.empty else "Monday"
        return [f"11:00 AM on {best_morning_day}", f"6:00 PM on {best_evening_day}"]

    results = []
    for _, row in grouped.iterrows():
        venue = row["venue"]
        time_suggestions = suggest_times(venue)
        results.append({
            "🏆 Venue": venue,
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
            "⏰ Best Times": time_suggestions
        })

    return {"results": results}












