import os
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
import pandas as pd
from datetime import datetime
from typing import Optional

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize app
app = FastAPI()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Topic mapping: normalize to abbreviations
TOPIC_MAP = {
    'taxes_in_retirement_567': 'TIR',
    'estate_planning_567': 'EP',
    'social_security_567': 'SS'
}

# Utility: calculate performance score
def calculate_score(row, recency_weight):
    if row["CPA"] == 0 or pd.isnull(row["CPA"]):
        return 0
    CPA_component = (1 / row["CPA"]) * 0.5
    fulfillment_component = row["Fulfillment %"] * 0.3
    attendance_component = row["Attendance Rate"] * 0.2
    raw_score = (CPA_component + fulfillment_component + attendance_component) * recency_weight
    return min(raw_score * 40, 40)

# Route: VOR endpoint
@app.get("/vor")
def get_top_venues(
    topic: str = Query(..., description="Seminar topic abbreviation: TIR, EP, or SS"),
    city: str = Query(...),
    state: str = Query(...),
    radius: int = Query(6)
):
    try:
        # Fetch data
        response = supabase.table("All Events 1").select("*").execute()
        df = pd.DataFrame(response.data)

        if df.empty:
            return {"error": "No data returned from Supabase."}

        # Normalize and map topic column
        df["Topic"] = df["Topic"].str.strip().str.lower().map(TOPIC_MAP)

        # Filter by topic
        df = df[df["Topic"] == topic.upper()]
        if df.empty:
            return {"message": f"No events found for topic {topic} in city {city}, state {state}."}

        # Convert date column
        df["Event_Date"] = pd.to_datetime(df["Event_Date"], errors='coerce')
        df = df.dropna(subset=["Event_Date"])

        # Location filtering
        df = df[df["City"].str.lower() == city.lower()]
        df = df[df["State"].str.lower() == state.lower()]
        if df.empty:
            return {"message": f"No matching venues found in {city}, {state}."}

        # Calculate metrics
        df["Attendance Rate"] = df["Attended_HH"] / df["Gross_Registrants"]
        df["Fulfillment %"] = df["Attended_HH"] / (df["Registration_Max"] / 2.4)
        df["CPA"] = df["CPR"] / df["Attendance Rate"]

        # Recency weight
        today = datetime.now()
        df["Days Ago"] = (today - df["Event_Date"]).dt.days
        df["Recency Weight"] = df["Days Ago"].apply(
            lambda x: 1.25 if x <= 30 else 1.0 if x <= 90 else 0.8
        )

        # Score venues
        df["Score"] = df.apply(lambda row: calculate_score(row, row["Recency Weight"]), axis=1)

        # Group and summarize
        summary = df.groupby("Venue").agg({
            "Event_Date": "max",
            "Job_Number": "count",
            "Gross_Registrants": "mean",
            "CPA": "mean",
            "CPR": "mean",
            "Attendance Rate": "mean",
            "Fulfillment %": "mean",
            "Score": "mean",
            "Venue_Image_Allowed": "first",
            "Venue_Disclosure_Needed": "first"
        }).reset_index()

        summary = summary.sort_values("Score", ascending=False).head(4)

        # Format response
        results = []
        for _, row in summary.iterrows():
            results.append({
                "🏅 Venue": row["Venue"],
                "📍 Location": f"{city.title()}, {state.upper()}",
                "📅 Most Recent Event": row["Event_Date"].strftime('%Y-%m-%d'),
                "🗓️ Event Count": int(row["Job_Number"]),
                "📈 Avg. Gross Registrants": round(row["Gross_Registrants"], 1),
                "💰 Avg. CPA": round(row["CPA"], 2),
                "💵 Avg. CPR": round(row["CPR"], 2),
                "📊 Attendance Rate": f"{round(row['Attendance Rate'] * 100, 1)}%",
                "🎯 Fulfillment %": f"{round(row['Fulfillment %'] * 100, 1)}%",
                "📸 Image Allowed": "✅" if row["Venue_Image_Allowed"] else "❌",
                "⚠️ Disclosure Needed": "✅" if row["Venue_Disclosure_Needed"] else "❌",
                "🥇 Score": f"{round(row['Score'], 2)}/40"
            })

        return {"results": results}

    except Exception as e:
        return {"error": str(e)}














