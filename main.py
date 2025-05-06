from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from supabase import create_client, Client
import pandas as pd
import os

app = FastAPI()

# 🔐 Load Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Cached global dataframe
df_clean = None

# Fields to treat as numeric
numeric_fields = [
    "CPA", "CPR", "Gross_Registrants", "Attended_HH", "Registration_Max"
]

# ⏳ Recency weight logic
def apply_recency_weight(row):
    days_since = (datetime.now() - pd.to_datetime(row["Event_Date"])).days
    if days_since <= 30:
        return 1.25
    elif 31 <= days_since <= 90:
        return 1.0
    else:
        return 0.8

# ✅ Supabase data loader & cleaner
def load_and_prepare_data():
    global df_clean
    response = supabase.table("all_events").select("*").execute()
    df = pd.DataFrame(response.data)

    # Normalize fields
    df["Topic"] = df["Topic"].str.strip().str.lower().replace({
        "taxes_in_retirement_567": "tir",
        "estate_planning_567": "ep",
        "social_security_567": "ss"
    })
    df["City"] = df["City"].str.strip().str.lower()
    df["State"] = df["State"].str.strip().str.upper()

    for field in numeric_fields:
        df[field] = pd.to_numeric(df[field], errors="coerce")

    # Derived metrics
    df["Attendance_Rate"] = df["Attended_HH"] / df["Gross_Registrants"]
    df["Fulfillment"] = df["Attended_HH"] / (df["Registration_Max"] / 2.4)
    df["Score"] = (
        (1 / df["CPA"]) * 0.5 +
        df["Fulfillment"] * 0.3 +
        df["Attendance_Rate"] * 0.2
    ) * 40
    df["Recency_Weight"] = df.apply(apply_recency_weight, axis=1)
    df["Score"] *= df["Recency_Weight"]

    df_clean = df

# 🚀 Trigger this when FastAPI app starts (Render-compatible)
@app.on_event("startup")
async def startup_event():
    print("✅ Loading and preparing data from Supabase...")
    load_and_prepare_data()
    print(f"✅ Loaded {len(df_clean)} rows from Supabase")

# 📥 Request body format
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: Optional[int] = 6

# 🧠 VOR endpoint (simple baseline logic)
@app.post("/vor")
def run_vor(request: VORRequest):
    if df_clean is None:
        return {"error": "Data not loaded."}

    topic = request.topic.strip().lower()
    city = request.city.strip().lower()
    state = request.state.strip().upper()

    filtered = df_clean[
        (df_clean["Topic"] == topic) &
        (df_clean["City"] == city) &
        (df_clean["State"] == state)
    ]

    if filtered.empty:
        return {"message": f"No results for {topic.upper()} in {city.title()}, {state}"}

    summary = filtered.groupby(["Venue", "City", "State"]).agg(
        Events=("Event_Date", "count"),
        Most_Recent_Event=("Event_Date", "max"),
        Avg_Registrants=("Gross_Registrants", "mean"),
        Avg_CPA=("CPA", "mean"),
        Avg_CPR=("CPR", "mean"),
        Avg_Attendance_Rate=("Attendance_Rate", "mean"),
        Avg_Fulfillment=("Fulfillment", "mean"),
        Avg_Score=("Score", "mean"),
        Image_Allowed=("Venue_Image_Allowed", "last"),
        Disclosure_Needed=("Venue_Disclosure_Needed", "last")
    ).reset_index().sort_values(by="Avg_Score", ascending=False)

    return summary.head(4).to_dict(orient="records")
















