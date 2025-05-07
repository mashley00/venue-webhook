from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from datetime import datetime

app = FastAPI()

CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

TOPIC_MAP = {
    "TIR": "taxes_in_retirement_567",
    "EP": "estate_planning_567",
    "SS": "social_security_567"
}

class VorRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: float = 6.0

@app.post("/vor")
def venue_optimization(request: VorRequest):
    try:
        df = pd.read_csv(CSV_URL, encoding='utf-8')
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load CSV: {e}")

    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)

    if df.empty:
        return {"message": "No data found."}

    # Normalize event_date and compute age
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df["event_age_days"] = (datetime.now() - df["event_date"]).dt.days

    # ✅ Map topic abbreviation to full name
    topic_key = request.topic.upper()
    topic_full = TOPIC_MAP.get(topic_key, request.topic.lower())
    df = df[df["topic"].str.lower() == topic_full]

    # Filter by city/state
    df = df[
        (df["city"].str.lower() == request.city.lower()) &
        (df["state"].str.lower() == request.state.lower())
    ]

    if df.empty:
        return {"message": "No matching rows for topic and city/state."}

    # Scoring logic
    df["attendance_rate"] = df["attended_hh"] / df["gross_registrants"]
    df["fulfillment_pct"] = df["attended_hh"] / (df["registration_max"] / 2.4)
    df["score"] = (
        (1 / df["cost_per_verified_hh"]) * 0.5 +
        df["fulfillment_pct"] * 0.3 +
        df["attendance_rate"] * 0.2
    )

    def weighted_score(row):
        if row["event_age_days"] <= 30:
            return row["score"] * 1.25 * 40
        elif row["event_age_days"] <= 90:
            return row["score"] * 1.0 * 40
        else:
            return row["score"] * 0.8 * 40

    df["weighted_score"] = df.apply(weighted_score, axis=1)

    # Aggregate by venue
    agg = df.groupby("venue").agg({
        "job_number": "count",
        "event_date": "max",
        "gross_registrants": "mean",
        "cost_per_verified_hh": "mean",
        "fb_cpr": "mean",
        "attendance_rate": "mean",
        "fulfillment_pct": "mean",
        "venue_image_allowed": "last",
        "venue_disclosure_needed": "last",
        "weighted_score": "mean"
    }).reset_index()

    agg = agg.rename(columns={
        "job_number": "event_count",
        "event_date": "most_recent",
        "gross_registrants": "avg_registrants",
        "cost_per_verified_hh": "avg_cpa",
        "fb_cpr": "avg_cpr",
        "attendance_rate": "attendance_rate",
        "fulfillment_pct": "fulfillment_pct",
        "weighted_score": "score"
    })

    agg = agg.sort_values(by="score", ascending=False).head(4)

    venues = []
    for _, row in agg.iterrows():
        venues.append({
            "🏆 Venue": row["venue"],
            "📍 City, State": f"{request.city}, {request.state}",
            "📅 Most Recent": row["most_recent"].strftime("%Y-%m-%d"),
            "🗓️ # Events": int(row["event_count"]),
            "📈 Avg Registrants": round(row["avg_registrants"], 1),
            "💰 Avg CPA": round(row["avg_cpa"], 2),
            "💵 Avg CPR": round(row["avg_cpr"], 2),
            "📉 Attendance Rate": f"{round(row['attendance_rate'] * 100, 1)}%",
            "🎯 Fulfillment %": f"{round(row['fulfillment_pct'] * 100, 1)}%",
            "📸 Image Allowed": "✅" if row["venue_image_allowed"] == True else "❌",
            "⚠️ Disclosure Needed": "✅" if row["venue_disclosure_needed"] == True else "❌",
            "🥇 Score": f"{round(row['score'], 1)} / 40"
        })

    return {"top_venues": venues}














