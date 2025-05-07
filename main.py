from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from datetime import datetime

app = FastAPI()

# ✅ Update with your working S3 CSV URL
CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

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

    # Preprocessing
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df["event_age_days"] = (datetime.now() - df["event_date"]).dt.days
    df = df[
        (df["topic"].str.lower() == request.topic.lower()) &
        (df["city"].str.lower() == request.city.lower()) &
        (df["state"].str.lower() == request.state.lower())
    ]

    if df.empty:
        return {"message": "No matching rows for topic and city/state."}

    # Calculated Fields
    df["attendance_rate"] = df["attended_hh"] / df["gross_registrants"]
    df["fulfillment_pct"] = df["attended_hh"] / (df["registration_max"] / 2.4)
    df["score"] = (
        (1 / df["cost_per_verified_hh"]) * 0.5 +
        df["fulfillment_pct"] * 0.3 +
        df["attendance_rate"] * 0.2
    )

    # Score weight
    def weighted_score(row):
        weight = 1.0
        if row["event_age_days"] <= 30:
            weight = 1.25
        elif 31 <= row["event_age_days"] <= 90:
            weight = 1.0
        else:
            weight = 0.8
        return row["score"] * weight * 40

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

    # Format response
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














