from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Union, Optional
import pandas as pd
import logging
from datetime import datetime

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VenueGPT")

# --- App Setup ---
app = FastAPI(
    title="Venue Optimization API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# --- Load dataset ---
CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

try:
    df = pd.read_csv(CSV_URL, encoding="utf-8")
    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
    df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
    logger.info(f"Loaded dataset: {df.shape}")
except Exception as e:
    logger.exception("Error loading dataset.")
    raise e

# --- Topic Mapping ---
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

# --- Schema ---
class VORRequest(BaseModel):
    topic: str = Field(..., description="Seminar topic code: TIR, EP, or SS")
    city: str
    state: str
    miles: Optional[Union[int, float]] = 6.0

@app.get("/health")
async def health_check():
    return {"status": "ok"}

# --- Scoring ---
def calculate_scores(df):
    df = df.copy()
    df.dropna(subset=['attended_hh', 'gross_registrants', 'registration_max', 'fb_cpr'], inplace=True)
    df['attendance_rate'] = df['attended_hh'] / df['gross_registrants']
    df['fulfillment_pct'] = df['attended_hh'] / (df['registration_max'] / 2.4)
    df['cpa'] = df['fb_cpr'] / df['attendance_rate']
    df['score'] = (1 / df['cpa'] * 0.5) + (df['fulfillment_pct'] * 0.3) + (df['attendance_rate'] * 0.2)
    df['score'] = df['score'] * 40
    return df

# --- Main Endpoint ---
@app.post("/vor", summary="Get Venue Optimization Report")
async def run_vor(request: VORRequest):
    logger.info(f"Received VOR request: {request.dict()}")

    try:
        input_topic = request.topic.strip()
        topic_key = input_topic.upper()

        if topic_key not in TOPIC_MAP:
            simplified = input_topic.lower().replace("_", " ").strip()
            fallback_code = TOPIC_MAP_REVERSE.get(simplified)
            topic_key = fallback_code or topic_key

        topic = TOPIC_MAP.get(topic_key)
        if not topic:
            raise HTTPException(status_code=400, detail="Invalid topic code. Use TIR, EP, or SS.")

        city = request.city.strip().lower()
        state = request.state.strip().upper()

        filtered = df[
            (df['topic'].str.strip().str.lower() == topic.lower()) &
            (df['city'].str.strip().str.lower() == city) &
            (df['state'].str.strip().str.upper() == state)
        ]

        if filtered.empty:
            raise HTTPException(status_code=404, detail="No matching events found.")

        scored = calculate_scores(filtered)
        today = pd.Timestamp.today()
        venues = []

        for venue_name, group in scored.groupby("venue"):
            group_sorted = group.sort_values("event_date", ascending=False)
            recent_event = group_sorted.iloc[0]

            used_recently = (today - recent_event['event_date']).days < 60
            disclosure = recent_event.get("venue_disclosure", "FALSE")
            image_ok = recent_event.get("image_allowed", "FALSE")

            pred_group = group.dropna(subset=[
                'fb_registrants', 'fb_days_running', 'fb_reach', 'fb_impressions'
            ])
            if not pred_group.empty:
                total_fb_reg = pred_group['fb_registrants'].sum()
                total_fb_days = pred_group['fb_days_running'].sum()
                total_reach = pred_group['fb_reach'].sum()
                total_impr = pred_group['fb_impressions'].sum()

                est_leads = round((total_fb_reg / total_fb_days) * 14) if total_fb_days else "N/A"
                reg_per_1k_reach = round(total_fb_reg / total_reach * 1000, 2) if total_reach else "N/A"
                reg_per_1k_impr = round(total_fb_reg / total_impr * 1000, 2) if total_impr else "N/A"
            else:
                est_leads = reg_per_1k_reach = reg_per_1k_impr = "N/A"

            preferred_times = ", ".join(sorted(set(group_sorted['event_time'].dropna()))) or "Not enough data"

            emoji_block = {
                "🥇 Venue": venue_name,
                "📍 City, State": f"{request.city}, {request.state}",
                "📅 Most Recent": recent_event['event_date'].strftime("%Y-%m-%d"),
                "🗓️ Number of Events": len(group),
                "📈 Avg. Gross Registrants": round(group['gross_registrants'].mean(), 1),
                "💵 Avg. CPR": f"${round(group['fb_cpr'].mean(), 2)}",
                "💰 Avg. CPA": f"${round(group['cpa'].mean(), 2)}",
                "📉 Attendance Rate": f"{round(group['attendance_rate'].mean() * 100, 1)}%",
                "🎯 Fulfillment %": f"{round(group['fulfillment_pct'].mean() * 100, 1)}%",
                "📸 Image Allowed": "✅" if image_ok == "TRUE" else "❌",
                "⚠️ Disclosure Needed": "✅" if disclosure == "TRUE" else "❌",
                "🚨 Recency Flag": "⚠️ Used <60d" if used_recently else "✅ OK",
                "⏰ Best Times": preferred_times,
                "🏅 Score": f"{round(group['score'].mean(), 2)} / 40",
                "🔮 Est. 14-Day Leads": est_leads,
                "📊 Reg/1k Reach": reg_per_1k_reach,
                "📊 Reg/1k Impressions": reg_per_1k_impr
            }

            venues.append(emoji_block)

        venues = sorted(venues, key=lambda v: float(v["🏅 Score"].split()[0]), reverse=True)[:4]
        return venues

    except Exception as e:
        logger.exception("Failed to process VOR.")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")








