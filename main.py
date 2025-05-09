from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
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

class VORRequest(BaseModel):
    topic: str
    city: str
    state: Optional[str] = None
    miles: Optional[Union[int, float]] = 6.0

@app.post("/vor")
async def run_vor(request: VORRequest):
    logger.info(f"Received VOR request: {request.dict()}")
    try:
        topic_key = request.topic.strip().upper()
        topic = TOPIC_MAP.get(topic_key)
        if not topic:
            raise HTTPException(status_code=400, detail="Invalid topic code. Use TIR, EP, or SS.")

        if request.city.isdigit() and len(request.city) == 5:
            filtered = df[(df['topic'] == topic) & (df['zip_code'].astype(str) == request.city)]
            display_city = filtered.iloc[0]['city'] if not filtered.empty else request.city
            display_state = filtered.iloc[0]['state'] if not filtered.empty else ""
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

        today = pd.Timestamp.today()
        filtered = filtered.copy()
        filtered['attendance_rate'] = filtered['attended_hh'] / filtered['gross_registrants']
        filtered['fulfillment_pct'] = filtered['attended_hh'] / (filtered['registration_max'] / 2.4)
        filtered['cpa'] = filtered['fb_cpr'] / filtered['attendance_rate']
        filtered['score'] = (1 / filtered['cpa'] * 0.5) + (filtered['fulfillment_pct'] * 0.3) + (filtered['attendance_rate'] * 0.2)
        filtered['score'] = filtered['score'] * 40

        preferred_times = ["11:00", "11:30", "18:00", "18:30"]
        venues = []

        for venue_name, group in filtered.groupby("venue"):
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

            base_times = time_scores[time_scores.index.isin(preferred_times)]
            extras = time_scores[~time_scores.index.isin(preferred_times)]
            good_times = pd.concat([
                base_times,
                extras[extras['cpa'] < 70],
                extras[extras['cpa'] < best_preferred_cpa]
            ]).drop_duplicates()
            best_times = ", ".join(sorted(good_times.index.tolist())) or "Not enough data"

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
                "image_allowed": "✅" if image_ok == "TRUE" else "❌",
                "disclosure_needed": "🟥" if disclosure == "TRUE" else "✅",
                "used_recently": "⚠️ Used <60d" if used_recently else "✅ OK",
                "best_days": best_days,
                "best_times": best_times,
                "score": round(group['score'].mean(), 2),
            })

        venues_sorted = sorted(venues, key=lambda x: float(x["score"]), reverse=True)
        top_venues = venues_sorted[:4]
        most_recent_venue = filtered.sort_values("event_date", ascending=False).iloc[0]

        response = ["**📊 Top Venues:**"]
        medals = ["🥇", "🥈", "🥉", "🏅"]

        for idx, venue in enumerate(top_venues):
            response.append(f"\n{medals[idx]} {venue['venue']}")
            response.append(f":round_pushpin: {venue['city']}, {venue['state']}")
            response.append(f":date: Most Recent – {venue['most_recent']}")
            response.append(f":spiral_calendar_pad: Events – {venue['num_events']}")
            response.append(f":chart_with_upwards_trend: Avg. Registrants – {venue['avg_gross']}")
            response.append(f":moneybag: Avg. CPA – {venue['avg_cpa']}")
            response.append(f":dollar: Avg. CPR – {venue['avg_cpr']}")
            response.append(f":chart_with_downwards_trend: Attendance Rate – {venue['attendance_rate']}")
            response.append(f":dart: Fulfillment % – {venue['fulfillment_pct']}")
            response.append(f":camera_with_flash: Image Allowed – {venue['image_allowed']}")
            response.append(f":warning: Disclosure Needed – {venue['disclosure_needed']}")
            response.append(f":rotating_light: Recency – {venue['used_recently']}")
            response.append(f":clock3: Best Times – {venue['best_times']} on {venue['best_days']}")

        response.append("\n**🕵️ Most Recently Used Venue in City:**")
        response.append(f"🏛️ {most_recent_venue['venue']}\n:date: {most_recent_venue['event_date'].strftime('%Y-%m-%d')}")

        response.append("\n**💬 Recommendation Summary:**")
        response.append(f"Top Pick: {top_venues[0]['venue']}")
        response.append("✅ Strong performance across attendance, cost, and registration efficiency.")
        response.append("📅 Suggest paired sessions at 11:00 AM and 6:00 PM on same day if possible.")

        return {"report": "\n".join(response)}

    except Exception as e:
        logger.exception("Failed to process VOR.")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")







