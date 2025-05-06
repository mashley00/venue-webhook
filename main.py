from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from supabase import create_client, Client
import pandas as pd
import os

# ✅ Log environment variables for debugging
print(f"✅ SUPABASE_URL raw: {repr(os.getenv('SUPABASE_URL'))}")
print(f"✅ SUPABASE_KEY raw: {repr(os.getenv('SUPABASE_KEY'))[:12]}...")

app = FastAPI()

# 🔐 Load Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 🔁 Cached global dataframe
df_clean = None

# 🔢 Fields to treat as numeric
numeric_fields = [
    "CPA", "CPR", "Gross_Registrants", "Attended_HH", "Registration_Max"
]

# 🕰️ Recency weight logic
def apply_recency_weight(row):
    days_ago = (datetime.now() - pd.to_datetime(row["Event_Date"])).days
    if days_ago <= 30:
        return 1.25
    elif 31 <= days_ago <= 90:
        return 1.0
    else:
        return 0.8

# ✅ Load and clean data
def load_and_prepare_data():
    global df_clean
    print("✅ Loading and preparing data from Supabase...")

    # 🔁 USE THE CORRECT TABLE NAME BELOW
    response = supabase.table("All Events 1").select("*").execute()
    rows = response.data

    df = pd.DataFrame(rows)
    if df.empty:
        raise Exception("⚠️ No data returned from Supabase.")

    # 🔢 Convert numeric fields
    for field in numeric_fields:
        df[field] = pd.to_numeric(df[field], errors="coerce")

    # 🧮 Derive additional metrics
    df["Attendance_Rate"] = df["Attended_HH"] / df["Gross_Registrants"]
    df["Fulfillment_Percent"] = df["Attended_HH"] / (df["Registration_Max"] / 2.4)
    df["Score"] = (
        (1 / df["CPA"]) * 0.5 +
        df["Fulfillment_Percent"] * 0.3 +
        df["Attendance_Rate"] * 0.2
    )
    df["Score"] = df["Score"] * df.apply(apply_recency_weight, axis=1) * 40
    df_clean = df
    print("✅ Data loaded and cleaned.")

# 🛠️ Startup event
@app.on_event("startup")
def startup_event():
    load_and_prepare_data()

# 📥 Request model
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: Optional[int] = 6

# 📤 Response model
@app.post("/vor")
def vor_query(req: VORRequest):
    if df_clean is None:
        return {"error": "Data not loaded"}

    df = df_clean.copy()

    # 🧵 Normalize topic input
    topic_map = {
        "tir": "taxes_in_retirement_567",
        "ep": "estate_planning_567",
        "ss": "social_security_567"
    }
    topic_key = req.topic.lower()
    topic_pattern = topic_map.get(topic_key, topic_key)

    # 📍 Filter
    mask = (
        df["Topic"].str.lower().str.contains(topic_pattern) &
        (df["City"].str.lower() == req.city.lower()) &
        (df["State"].str.lower() == req.state.lower())
    )
    df_filtered = df[mask]

    if df_filtered.empty:
        return {
            "message": f"No results found for topic '{req.topic.upper()}' in {req.city}, {req.state}."
        }

    top_venues = (
        df_filtered
        .groupby("Venue")
        .agg({
            "Event_Date": "max",
            "Gross_Registrants": "mean",
            "CPA": "mean",
            "CPR": "mean",
            "Attendance_Rate": "mean",
            "Fulfillment_Percent": "mean",
            "Score": "mean",
            "Venue_Image_Allowed": "last",
            "Venue_Disclosure_Needed": "last",
            "City": "last",
            "State": "last"
        })
        .sort_values("Score", ascending=False)
        .head(4)
        .reset_index()
    )

    results = []
    for _, row in top_venues.iterrows():
        results.append({
            "venue": row["Venue"],
            "city": row["City"],
            "state": row["State"],
            "most_recent_event": row["Event_Date"],
            "avg_gross_registrants": round(row["Gross_Registrants"], 1),
            "avg_cpa": round(row["CPA"], 2),
            "avg_cpr": round(row["CPR"], 2),
            "attendance_rate": f"{round(row['Attendance_Rate'] * 100, 1)}%",
            "fulfillment_percent": f"{round(row['Fulfillment_Percent'] * 100, 1)}%",
            "score": f"{round(row['Score'], 1)}/40",
            "image_allowed": "✅" if str(row["Venue_Image_Allowed"]).lower() == "true" else "❌",
            "disclosure_needed": "✅" if str(row["Venue_Disclosure_Needed"]).lower() == "true" else "❌"
        })

    return {"top_venues": results}















