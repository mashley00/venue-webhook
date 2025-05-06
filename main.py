from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from supabase import create_client, Client
import pandas as pd
import os

# ✅ Log environment variables for debugging
print(f"✅ SUPABASE_URL raw: {repr(os.getenv('SUPABASE_URL'))}")
print(f"✅ SUPABASE_KEY raw: {repr(os.getenv('SUPABASE_KEY')[:12])}...")

app = FastAPI()

# 🗝️ Load Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 📦 Cached global dataframe
df_clean = None

# 📊 Fields to treat as numeric
numeric_fields = [
    "CPA", "CPR", "Gross_Registrants", "Attended_HH", "Registration_Max"
]

# ⏳ Recency weight logic
def apply_recency_weight(days_old: int) -> float:
    if days_old <= 30:
        return 1.25
    elif days_old <= 90:
        return 1.0
    else:
        return 0.8

# 🚀 Startup event to load and clean data
@app.on_event("startup")
def startup_event():
    print("✅ Loading and preparing data from Supabase...")
    load_and_prepare_data()

# 🧹 Load and clean Supabase data
def load_and_prepare_data():
    global df_clean
    response = supabase.table("All Events 1").select("*").execute()
    if not response.data:
        raise Exception("⚠️ No data returned from Supabase.")

    df = pd.DataFrame(response.data)

    # ✅ Rename columns for standardization
    df.columns = df.columns.str.replace(" ", "_").str.replace("-", "_")

    # 🎯 Normalize topic
    if "Topic" in df.columns:
        df["Topic"] = df["Topic"].str.lower().str.strip()
        df["Topic"] = df["Topic"].replace({
            "taxes_in_retirement_567": "TIR",
            "estate_planning_567": "EP",
            "social_security_567": "SS",
        })

    # 🔢 Convert numeric fields
    for col in numeric_fields:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 📅 Convert Event_Date to datetime
    if "Event_Date" in df.columns:
        df["Event_Date"] = pd.to_datetime(df["Event_Date"], errors="coerce")

    # 📉 Filter out empty events
    df_clean = df.dropna(subset=["Event_Date", "Topic", "City", "State"])
    print(f"✅ Loaded {len(df_clean)} valid rows.")

# 🔍 Search request model
class SearchRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: Optional[int] = 6

# 📡 Basic ping
@app.get("/")
def read_root():
    return {"message": "Venue Option-INATOR is live!"}

# 🔍 Example endpoint
@app.post("/search")
def search_venues(request: SearchRequest):
    return {
        "status": "OK",
        "input": {
            "topic": request.topic,
            "city": request.city,
            "state": request.state,
            "miles": request.miles
        },
        "results": []
    }















