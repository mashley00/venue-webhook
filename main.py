from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from supabase import create_client, Client
import pandas as pd
import os

# ✅ Log environment variables for debugging
print(f"✅ SUPABASE_URL raw: {repr(os.getenv('SUPABASE_URL', '')[:50])}")
print(f"✅ SUPABASE_KEY raw: {repr(os.getenv('SUPABASE_KEY', '')[:12])}...")

app = FastAPI()

# 🗝️ Load Supabase credentials from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 🧠 Cached global dataframe
df_clean = None

# 🔢 Fields to treat as numeric
numeric_fields = [
    "CPA", "CPR", "Gross_Registrants", "Attended_HH", "Registration_Max"
]

# ⏳ Recency weight logic
def apply_recency_weight(event_date_str: str) -> float:
    try:
        event_date = datetime.strptime(event_date_str, "%A, %B %d, %Y")
        days_ago = (datetime.now() - event_date).days
        if days_ago <= 30:
            return 1.25
        elif days_ago <= 90:
            return 1.0
        else:
            return 0.8
    except Exception:
        return 1.0

# 🚀 Data load on startup
@app.on_event("startup")
def startup_event():
    load_and_prepare_data()

# 📥 Load & clean Supabase data
def load_and_prepare_data():
    global df_clean
    print("✅ Loading and preparing data from Supabase...")

    # ⚠️ Fix: quoted table name for case/space-sensitive access
    response = supabase.table('"All Events 1"').select("*").execute()
    
    if not response.data:
        raise Exception("⚠️ No data returned from Supabase.")

    df = pd.DataFrame(response.data)

    # ⛏️ Clean column names (optional, depending on your structure)
    df.columns = [col.strip().replace(" ", "_").replace("-", "_") for col in df.columns]

    # 🧽 Convert numerics
    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors="coerce")

    # 🧮 Derived metrics
    if all(col in df.columns for col in ["Attended_HH", "Gross_Registrants"]):
        df["Attendance_Rate"] = df["Attended_HH"] / df["Gross_Registrants"]

    if all(col in df.columns for col in ["CPA", "Registration_Max", "Attended_HH"]):
        df["Fulfillment_Rate"] = df["Attended_HH"] / (df["Registration_Max"] / 2.4)

    # 🎯 Apply recency weighting
    df["Recency_Weight"] = df["Event_Date"].apply(apply_recency_weight)

    # 🧼 Drop rows with essential missing values
    df_clean = df.dropna(subset=["Venue", "City", "State", "Topic"])

    print(f"✅ Loaded {len(df_clean)} cleaned records.")

# 📡 Health check route
@app.get("/")
def root():
    return {"message": "AU Venue-Option-INATOR is running and ready! ✅"}















