import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from supabase import create_client, Client
from datetime import datetime
from geopy.distance import geodesic

import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

print(f"✅ SUPABASE_URL raw: '{SUPABASE_URL}'")
print(f"✅ SUPABASE_KEY raw: '{SUPABASE_KEY[:10]}...'")  # Print first 10 chars only

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust as needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

df = pd.DataFrame()

def clean_columns(df):
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
    return df

def load_and_prepare_data():
    print("✅ Loading and preparing data from Supabase...")
    response = supabase.table("all_events").select("*").execute()

    if not response.data:
        raise Exception("⚠️ No data returned from Supabase.")

    global df
    df = pd.DataFrame(response.data)

    if df.empty:
        raise Exception("⚠️ Dataframe is empty after Supabase load.")

    df = clean_columns(df)

    # Parse date fields
    for date_col in ["Event_Date", "Marketing_Start_Date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    for num_col in [
        "Registration_Max", "Fulfillment_Percent", "Net_Registrants", "Gross_Registrants", "FB_Registrants",
        "FB_CPR", "Attended_HH", "Walk_Ins", "Attendance_Rate", "CPA", "FB_Days_Running", "FB_Impressions",
        "CPM", "FB_Reach"
    ]:
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors="coerce")

    print(f"✅ Loaded {len(df)} rows with columns: {list(df.columns)}")

@app.on_event("startup")
def startup_event():
    load_and_prepare_data()

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/preview")
def preview_data(limit: int = 10):
    return df.head(limit).to_dict(orient="records")

class VorRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: float = 6.0

@app.post("/vor")
def venue_optimization(request: VorRequest):
    # Placeholder for future VOR logic
    return {"message": f"VOR request received for {request.topic} in {request.city}, {request.state} within {request.miles} miles."}













