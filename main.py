from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from datetime import datetime

app = FastAPI()

CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: float = 6.0

class VORResponse(BaseModel):
    venue: str
    city: str
    state: str
    most_recent_event: str
    number_of_events: int
    avg_gross_registrants: float
    avg_cpa: float
    avg_cpr: float
    attendance_rate: float
    fulfillment_pct: float
    image_allowed: bool
    disclosure_needed: bool
    score: float
    best_time_1: str
    best_time_2: str

@app.post("/vor", response_model=list[VORResponse])
def get_vor(request: VORRequest):
    try:
        df = pd.read_csv(CSV_URL, encoding="utf-8")
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace(r"[^\w\s]", "", regex=True)
        )

        required_cols = [
            'event_date', 'venue', 'job_number', 'gross_registrants',
            'cost_per_verified_hh', 'fb_cpr', 'attended_hh',
            'registration_max', 'venue_image_allowed', 'venue_disclosure_needed',
            'topic', 'city', 'state'
        ]

        for col in required_cols:
            if col not in df.columns:
                raise HTTPException(status_code=500, detail=f"Missing required column: {col}")

        # Normalize topic mapping
        topic_map = {
            "TIR": "taxes_in_retirement_567",
            "EP": "estate_planning_567",
            "SS": "social_security_567"
        }

        target_topic = topic_map.get(request.topic.upper(), request.topic.lower())

        df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
        df["event_age_days"] = (datetime.now() - df["event_date"]).dt.days

        filtered = df[
            (df["topic"].str.lower() == target_topic) &
            (df["city"].str.lower() == request.city.lower()) &
            (df["state"].str.lower() == request.state.lower())
        ]

        if filtered.empty:
            return []

        filtered["attendance_rate"] = filtered["attended_hh"] / filtered["gross_registrants"]
        filtered["fulfillment_pct"] = filtered["attended_hh"] / (filtered["registration_max"] / 2.4)
        filtered["score"] = (
            (1 / filtered["cost_per_verified_hh"]) * 0.5 +
            filtered["fulfillment_pct"] * 0.3 +
            filtered["attendance_rate"] * 0.2
        )

        def weighted_score(row):
            weight = 1.0
            if row["event_age_days"] <= 30:
                weight = 1.25
            elif 31 <= row["event_age_days"] <= 90:
                weight = 1.0
            else:
                weight = 0.8
            return row["score"] * weight * 100  # ⬅️ Score is now on a 100-point scale

        filtered["weighted_score"] = filtered.apply(weighted_score, axis=1)

        print("DEBUG: Available columns for aggregation:", filtered.columns.tolist())

        grouped = filtered.groupby("venue").agg({
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

        grouped = grouped.rename(columns={
            "job_number": "number_of_events",
            "event_date": "most_recent_event",
            "gross_registrants": "avg_gross_registrants",
            "cost_per_verified_hh": "avg_cpa",
            "fb_cpr": "avg_cpr",
            "attendance_rate": "attendance_rate",
            "fulfillment_pct": "fulfillment_pct",
            "weighted_score": "score"
        })

        grouped = grouped.sort_values(by="score", ascending=False).head(4)

        results = []
        for _, row in grouped.iterrows():
            results.append(VORResponse(
                venue=row["venue"],
                city=request.city,
                state=request.state,
                most_recent_event=row["most_recent_event"].strftime("%Y-%m-%d") if not pd.isnull(row["most_recent_event"]) else "N/A",
                number_of_events=int(row["number_of_events"]),
                avg_gross_registrants=round(row["avg_gross_registrants"], 1),
                avg_cpa=round(row["avg_cpa"], 2),
                avg_cpr=round(row["avg_cpr"], 2),
                attendance_rate=round(row["attendance_rate"], 3),
                fulfillment_pct=round(row["fulfillment_pct"], 3),
                image_allowed=bool(row["venue_image_allowed"]),
                disclosure_needed=bool(row["venue_disclosure_needed"]),
                score=round(row["score"], 1),
                best_time_1="11:00am Monday",
                best_time_2="6:00pm Monday"
            ))

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))















