import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
import pandas as pd
from geopy.distance import geodesic
import numpy as np
from datetime import datetime

# Initialize FastAPI app
app = FastAPI()

# Enable CORS for all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables
supabase: Client = None
df: pd.DataFrame = None
topic_mapping = {
    "tir": "taxes_in_retirement_567",
    "ep": "estate_planning_567",
    "ss": "social_security_567",
}
TOPIC_NAMES = {
    "tir": "Taxes in Retirement",
    "ep": "Estate Planning",
    "ss": "Social Security",
}

# Load environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

print("✅ SUPABASE_URL raw:", repr(SUPABASE_URL))
print("✅ SUPABASE_KEY raw:", repr(SUPABASE_KEY[:10]) + "..." if SUPABASE_KEY else "❌ MISSING")
print("✅ Loading and preparing data from Supabase...")

# Data Loading and Preprocessing
def load_and_prepare_data():
    global df

    # Fetch data from Supabase
    response = supabase.table("all_events_1").select("*").execute()
    data = response.data
    print(f"✅ Retrieved {len(data)} rows from Supabase.")

    if not data:
        raise Exception("⚠️ No data returned from Supabase.")

    df_raw = pd.DataFrame(data)

    # Rename columns for consistency
    df_raw = df_raw.rename(columns=lambda x: x.strip().replace(" ", "_").replace("-", "_").lower())

    # Normalize topic
    df_raw["topic"] = df_raw["topic"].str.lower().str.strip()

    # Standardize column names
    if "fb_cpr" in df_raw.columns:
        df_raw.rename(columns={"fb_cpr": "cpr"}, inplace=True)

    if "cost_per_verified_hh" not in df_raw.columns and "cpa" in df_raw.columns:
        df_raw.rename(columns={"cpa": "cost_per_verified_hh"}, inplace=True)

    # Convert numeric columns
    for col in ["cpr", "cost_per_verified_hh", "attended_hh", "gross_registrants", "registration_max"]:
        if col in df_raw.columns:
            df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce")

    # Drop rows missing key values
    df_raw = df_raw.dropna(subset=["venue", "city", "state", "latitude", "longitude", "event_date"])

    # Add computed metrics
    df_raw["attendance_rate"] = df_raw["attended_hh"] / df_raw["gross_registrants"]
    df_raw["fulfillment_pct"] = df_raw["attended_hh"] / (df_raw["registration_max"] / 2.4)
    df_raw["event_date"] = pd.to_datetime(df_raw["event_date"])

    df = df_raw
    print("✅ Data loaded and cleaned. Rows:", len(df))


# Models
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    radius: int = 6


# Startup
@app.on_event("startup")
def startup_event():
    global supabase
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    load_and_prepare_data()


# Health Check
@app.get("/")
def root():
    return {"message": "Venue Option-INATOR is live!"}


# VOR Endpoint
@app.post("/vor")
def get_vor(request: VORRequest):
    global df

    topic_key = request.topic.lower()
    topic_column_value = topic_mapping.get(topic_key)
    if topic_column_value is None:
        raise HTTPException(status_code=400, detail="Invalid topic.")

    df_filtered = df[
        (df["topic"] == topic_column_value) &
        (df["city"].str.lower() == request.city.lower()) &
        (df["state"].str.lower() == request.state.lower())
    ]

    if df_filtered.empty:
        raise HTTPException(status_code=404, detail="No events found for given criteria.")

    results = df_filtered.head(4).to_dict(orient="records")
    return {
        "topic": TOPIC_NAMES[topic_key],
        "city": request.city,
        "state": request.state,
        "radius_miles": request.radius,
        "results": results
    }













