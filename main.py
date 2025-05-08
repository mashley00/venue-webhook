from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Union, Optional
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VenueGPT")

app = FastAPI(title="Venue Optimization API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

try:
    df = pd.read_csv(CSV_URL, encoding="utf-8")
    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
    df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
    df['event_day'] = df['event_date'].dt.day_name()
    df['event_time'] = df['event_time'].str.strip()
    logger.info(f"Loaded dataset: {df.shape}")
except Exception as e:
    logger.exception("Error loading dataset.")
    raise e

TOPIC_MAP = {
    "TIR": "taxes_in_retirement_567",
    "EP": "estate_planning_567",
    "SS": "social_security_567"
}

TOPIC_MAP_REVERSE = {
    "taxes in retirement": "TIR",
    "taxes_in_retirement_567": "TIR",
    "estate planning": "EP",
    "estate_planning_567": "EP",
    "social security": "SS",
    "social_security_567": "SS"
}

class VORRequest(BaseModel):
    topic: str
    city: str
    state: Optional[str] = None
    miles: Optional[Union[int, float]] = 6.0

@app.get("/health")
async def health_check():
    return {"status": "ok"}

def calculate_scores(df):
    df = df.copy()
    df.dropna(subset=['attended_hh', 'gross_registrants', 'registration_max', 'fb_cpr'], inplace=True)
    df['attendance_rate'] = df['attended_hh'] / df['gross_registrants']
    df['fulfillment_pct'] = df['attended_hh'] / (df['registration_max'] / 2.4)
    df['cpa'] = df['fb_cpr'] / df['attendance_rate']
    df['score'] = (1 / df['cpa'] * 0.5) + (df['fulfillment_pct'] * 0.3) + (df['attendance_rate'] * 0.2)
    df['score'] = df['score'] * 40
    return df

@app.post("/vor")
async def run_vor(request: VORRequest):
    logger.info(f"Received VOR request: {request.dict()}")
    try:
        topic_key = request.topic.strip().upper()
        topic = TOPIC_MAP.get(topic_key) or TOPIC_MAP.get(TOPIC_MAP_REVERSE.get(topic_key.lower()))
        if not topic:
            raise HTTPException(status_code=400, detail="Invalid topic code. Use TIR, EP, or SS.")

        filtered = pd.DataFrame()
        display_city, display_state = None, None

        if request.city.isdigit() and len(request.city) == 5:
            filtered = df[(df['topic'] == topic) & (df['zip_code'].astype(str) == request.city)]
            if not filtered.empty:
                display_city = filtered.iloc[0]['city']
                display_state = filtered.iloc[0]['state']
        else:
            city = request.city.strip().lower()
            state = request.state.strip().upper()
            filtered = df[
                (df['topic'] == topic) &
                (df['city'].str.strip().str.lower() == city) &
                (df['state'].str.strip().str.upper() == state)
            ]
            display_city = request.city.strip().title()
            display_state = state

        if filtered.empty:
            raise HTTPException(status_code=404, detail="No matching events found.")

        scored = calculate_scores(filtered)
        today = pd.Timestamp.today()
        venues = []

        preferred_times = ["11:00", "11:30", "18:00", "18:30"]

        for venue_name, group in scored.groupby("venue"):
            group_sorted = group.sort_values("event_date", ascending=False)
            recent_event = group_sorted.iloc[0]
            used_recently = (today - recent_event['event_date']).days < 60
            disclosure = recent_event.get("venue_disclosure", "FALSE")
            image_ok = recent_event.get("image_allowed", "FALSE")

            best_day_scores = group.groupby("event_day").apply(
                lambda x: (x['attendance_rate'].mean() + x['fulfillment_pct'].mean()) / 2
            ).sort_values(ascending=False)
            best_days = ", ".join(best_day_scores.head(2).index.tolist())

            time_scores = group.groupby("event_time").agg({
                'fb_cpr': 'mean',
                'attendance_rate': 'mean'
            }).dropna()
            time_scores['cpa'] = time_scores['fb_cpr'] / time_scores['attendance_rate']
            preferred_cpa = time_scores.loc[time_scores.index.isin(preferred_times), 'cpa']
            best_preferred_cpa = preferred_cpa.min() if not preferred_cpa.empty else 9999

            good_times = time_scores[time_scores.index.isin(preferred_times)].copy()
            good_times = good_times.append(
                time_scores[~time_scores.index.isin(preferred_times)][
                    time_scores['cpa'] < 70
                ]
            )
            good_times = good_times.append(
                time_scores[~time_scores.index.isin(preferred_times)][
                    time_scores['cpa'] < best_preferred_cpa
                ]
            )
            good_times = good_times[~good_times.index.duplicated()]
            best_times = ", ".join(sorted(good_times.index.tolist())) or "Not enough data"

            venues.append({
                "🥇 Venue": venue_name,
                "📍 City, State": f"{display_city}, {display_state}",
                "📅 Most Recent": recent_event['event_date'].strftime("%Y-%m-%d"),
                "🗓️ Number of Events": len(group),
                "📈 Avg. Gross Registrants": round(group['gross_registrants'].mean(), 1),
                "💵 Avg. CPR": f"${round(group['fb_cpr'].mean(), 2)}",
                "💰 Avg. CPA": f"${round(group['cpa'].mean(), 2)}",
                "📉 Attendance Rate": f"{round(group['attendance_rate'].mean() * 100, 1)}%",
                "🎯 Fulfillment %": f"{round(group['fulfillment_pct'].mean() * 100, 1)}%",
                "📸 Image Allowed": "✅" if image_ok == "TRUE" else "❌",
                "⚠️ Disclosure Needed": "🟥" if disclosure == "TRUE" else "✅",
                "🚨 Recency Flag": "⚠️ Used <60d" if used_recently else "✅ OK",
                "📅 Best Days": best_days,
                "⏰ Best Times": best_times,
                "🏅 Score": f"{round(group['score'].mean(), 2)} / 40",
            })

        return sorted(venues, key=lambda x: float(x["🏅 Score"].split()[0]), reverse=True)[:4]

    except Exception as e:
        logger.exception("Failed to process VOR.")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")







