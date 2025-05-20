from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Union, Optional
from datetime import datetime
from fuzzywuzzy import fuzz
import pandas as pd
import logging

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VenueGPT")

# FastAPI app
app = FastAPI(title="Venue Optimization API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Dataset load
CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
try:
    df = pd.read_csv(CSV_URL, encoding="utf-8")
    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
    df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
    df['event_day'] = df['event_date'].dt.day_name()
    df['event_time'] = df['event_time'].astype(str).str.strip()
    df['zip_code'] = df.get('zip_code', '').fillna('').astype(str).str.strip().str.zfill(5)
    logger.info(f"Loaded dataset: {df.shape}")
except Exception as e:
    logger.exception("Error loading dataset.")
    raise e

# Topic codes
TOPIC_MAP = {
    "TIR": "taxes_in_retirement_567",
    "EP": "estate_planning_567",
    "SS": "social_security_567"
}

class VORRequest(BaseModel):
    topic: str
    city: str
    state: Optional[str] = None
    miles: Optional[Union[int, float]] = 6.0

def is_true(val):
    return str(val).strip().upper() == "TRUE"

def get_similar_cities(input_city, state, threshold=75):
    normalized_city = input_city.strip().lower()
    candidates = df[df['state'].str.upper().str.strip() == state]['city'].dropna().unique()
    return [city for city in candidates if fuzz.token_set_ratio(normalized_city, city.lower().strip()) >= threshold]

@app.post("/vor")
async def run_vor(request: VORRequest):
    try:
        topic_key = request.topic.strip().upper()
        topic = TOPIC_MAP.get(topic_key)
        if not topic:
            raise HTTPException(status_code=400, detail="Invalid topic code. Use TIR, EP, or SS.")

        if request.city.isdigit() and len(request.city) == 5:
            zip_str = request.city.zfill(5)
            filtered = df[(df['topic'] == topic) & (df['zip_code'] == zip_str)]
            display_city = filtered.iloc[0]['city'] if not filtered.empty else zip_str
            display_state = filtered.iloc[0]['state'] if not filtered.empty else ""
        else:
            city = request.city.strip()
            state = request.state.strip().upper()
            matches = get_similar_cities(city, state)
            if not matches:
                raise HTTPException(status_code=404, detail="No similar city matches found.")
            filtered = df[
                (df['topic'] == topic) &
                (df['city'].str.strip().str.lower().isin([c.lower() for c in matches])) &
                (df['state'].str.upper().str.strip() == state)
            ]
            display_city = ", ".join(sorted(set(c.title() for c in matches)))
            display_state = state

        if filtered.empty:
            raise HTTPException(status_code=404, detail="No matching events found.")

        today = pd.Timestamp.today()
        filtered = filtered.copy()
        filtered['attendance_rate'] = filtered['attended_hh'] / filtered['gross_registrants']
        filtered['fulfillment_pct'] = filtered['attended_hh'] / (filtered['registration_max'] / 2.4)
        filtered['cpa'] = filtered['fb_cpr'] / filtered['attendance_rate']
        filtered['score'] = (1 / filtered['cpa'] * 0.5) + (filtered['fulfillment_pct'] * 0.3) + (filtered['attendance_rate'] * 0.2)
        filtered['score'] *= 40

        preferred_times = ["11:00", "11:30", "18:00", "18:30"]
        venues = []

        for venue_name, group in filtered.groupby("venue"):
            group = group.sort_values("event_date", ascending=False)
            recent_event = group.iloc[0]
            used_recently = (today - recent_event['event_date']).days < 60
            disclosure = is_true(recent_event.get("venue_disclosure"))
            image_ok = is_true(recent_event.get("image_allowed"))

            best_days = ", ".join(
                group.groupby("event_day").apply(
                    lambda x: (x['attendance_rate'].mean() + x['fulfillment_pct'].mean()) / 2
                ).sort_values(ascending=False).head(2).index
            )

            time_scores = group.groupby("event_time").agg({'fb_cpr': 'mean', 'attendance_rate': 'mean'}).dropna()
            time_scores['cpa'] = time_scores['fb_cpr'] / time_scores['attendance_rate']
            base_times = time_scores[time_scores.index.isin(preferred_times)]
            extras = time_scores[~time_scores.index.isin(preferred_times)]
            best_times = ", ".join(pd.concat([
                base_times,
                extras[extras['cpa'] < 70]
            ]).index.drop_duplicates())

            venues.append({
                "venue": venue_name,
                "city": display_city,
                "state": display_state,
                "most_recent": recent_event['event_date'].strftime("%Y-%m-%d"),
                "num_events": len(group),
                "avg_gross": round(group['gross_registrants'].mean(), 1),
                "avg_cpr": f"${round(group['fb_cpr'].mean(), 2)}",
                "avg_cpa": f"${round(group['cpa'].mean(), 2)}",
                "attendance_rate": f"{round(group['attendance_rate'].mean() * 100, 1)}%",
                "fulfillment_pct": f"{round(group['fulfillment_pct'].mean() * 100, 1)}%",
                "image_allowed": "✅" if image_ok else "❌",
                "disclosure_needed": "🟥" if disclosure else "✅",
                "used_recently": "⚠️ Used <60d" if used_recently else "✅ OK",
                "best_days": best_days,
                "best_times": best_times,
                "score": round(group['score'].mean(), 2),
            })

        top_venues = sorted(venues, key=lambda v: v["score"], reverse=True)[:4]
        recent = filtered.sort_values("event_date", ascending=False).iloc[0]

        medals = ["🥇", "🥈", "🥉", "🏅"]
        response = [
            f"🕵️ Most Recently Used Venue in City:",
            f"🏛️ {recent['venue']}",
            f"📅 {recent['event_date'].strftime('%Y-%m-%d')}",
            f"**📊 Top Venues:**",
            f"🔎 Included city variations: {display_city}"
        ]

        for i, v in enumerate(top_venues):
            response.extend([
                f"{medals[i]} {v['venue']}",
                f"📍 {v['city']}, {v['state']}",
                f"📅 Most Recent – {v['most_recent']}",
                f"🗓️ Events – {v['num_events']}",
                f"📈 Avg. Registrants – {v['avg_gross']}",
                f"💰 Avg. CPA – {v['avg_cpa']}",
                f"💵 Avg. CPR – {v['avg_cpr']}",
                f"📉 Attendance Rate – {v['attendance_rate']}",
                f"🎯 Fulfillment % – {v['fulfillment_pct']}",
                f"📸 Image Allowed – {v['image_allowed']}",
                f"⚠️ Disclosure Needed – {v['disclosure_needed']}",
                f"⚠️ Recency – {v['used_recently']}",
                f"🕒 Best Times – {v['best_times']} on {v['best_days']}",
                "---"
            ])

        if top_venues:
            response.extend([
                "**💬 Recommendation Summary:**",
                f"Top Pick: {top_venues[0]['venue']}",
                "✅ Strong performance across attendance, cost, and registration efficiency.",
                "📅 Suggest paired sessions at 11:00 AM and 6:00 PM on same day if possible."
            ])

        return {"report": "\n".join(response)}

    except Exception as e:
        logger.exception("VOR processing failed")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

# Static HTML rendering endpoints
@app.get("/market.html", response_class=HTMLResponse)
async def serve_market():
    with open("static/market.html", "r") as f:
        return HTMLResponse(content=f.read(), status_code=200)

@app.get("/predict.html", response_class=HTMLResponse)
async def serve_predict():
    with open("static/predict.html", "r") as f:
        return HTMLResponse(content=f.read(), status_code=200)

# Mount static folder
app.mount("/static", StaticFiles(directory="static", html=True), name="static")














