from fastapi import FastAPI, Request
from pydantic import BaseModel
import pandas as pd
from datetime import datetime, timedelta
import math
import requests

app = FastAPI()

S3_CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

class VORRequest(BaseModel):
    topic: str  # "TIR", "EP", or "SS"
    city: str
    state: str
    miles: float = 6.0

@app.post("/vor")
async def venue_optimization(req: VORRequest):
    try:
        # Load DataFrame from S3
        df = pd.read_csv(S3_CSV_URL)

        # Clean and normalize columns
        df.columns = df.columns.str.strip()
        df = df[df['Topic'].str.upper() == req.topic.upper()]
        df = df[df['City'].str.upper() == req.city.upper()]
        df = df[df['State'].str.upper() == req.state.upper()]
        df = df[df['Miles from center'] <= req.miles]

        if df.empty:
            return {"message": "No matching venues found."}

        df['Event Date'] = pd.to_datetime(df['Event Date'], errors='coerce')
        df = df.dropna(subset=['Event Date'])

        today = pd.Timestamp.now()
        df['Days Ago'] = (today - df['Event Date']).dt.days

        # Calculate derived fields
        df['Attendance Rate'] = df['Attended HH'] / df['Gross Registrants']
        df['Fulfillment %'] = df['Attended HH'] / (df['Registration Max'] / 2.4)
        df['Score Weight'] = df['Days Ago'].apply(lambda d: 1.25 if d <= 30 else (1.0 if d <= 90 else 0.8))

        # Avoid divide-by-zero errors
        df['CPA'] = df['Cost per Verified HH'].replace(0, 9999)
        df['CPR'] = df['FB CPR'].replace(0, 9999)

        # Scoring formula
        df['Score Raw'] = (1 / df['CPA'] * 0.5) + (df['Fulfillment %'] * 0.3) + (df['Attendance Rate'] * 0.2)
        df['Final Score'] = df['Score Raw'] * df['Score Weight'] * 40  # Scale to 40-point system

        # Aggregate by venue
        group_cols = ['Venue']
        agg_df = df.groupby(group_cols).agg({
            'Final Score': 'mean',
            'Event Date': 'max',
            'Gross Registrants': 'mean',
            'CPA': 'mean',
            'CPR': 'mean',
            'Attendance Rate': 'mean',
            'Fulfillment %': 'mean',
            'Venue Image Allowed': 'first',
            'Disclosure Required': 'first',
            'Job Number': 'count'
        }).reset_index()

        agg_df = agg_df.sort_values(by='Final Score', ascending=False).head(4)

        # Format response
        results = []
        for _, row in agg_df.iterrows():
            results.append({
                "🏆 Venue": row['Venue'],
                "📍 Location": f"{req.city}, {req.state}",
                "📅 Most Recent Event": row['Event Date'].strftime("%Y-%m-%d"),
                "📆 Number of Events": int(row['Job Number']),
                "📈 Avg. Gross Registrants": round(row['Gross Registrants'], 1),
                "💰 Avg. CPA": f"${row['CPA']:.2f}",
                "💵 Avg. CPR": f"${row['CPR']:.2f}",
                "📉 Attendance Rate": f"{row['Attendance Rate']:.1%}",
                "🎯 Fulfillment %": f"{row['Fulfillment %']:.1%}",
                "📸 Image Allowed": "✅" if row['Venue Image Allowed'] == "Yes" else "❌",
                "⚠️ Disclosure Needed": "✅" if row['Disclosure Required'] == "Yes" else "❌",
                "🥇 Score": f"{row['Final Score']:.1f}/40",
            })

        return {"status": "success", "top_venues": results}

    except Exception as e:
        return {"error": str(e)}
















