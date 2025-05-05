from fastapi import FastAPI, Query
from supabase import create_client, Client
from typing import Optional
import pandas as pd
import datetime
import uvicorn

# Supabase credentials
SUPABASE_URL = "https://drcjaimdtalwvpvqbdmb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRyY2phaW1kdGFsd3ZwdnFiZG1iIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDY0NTgxMzMsImV4cCI6MjA2MjAzNDEzM30.9ztc3baZzHlgrCYHyDeUG7xHiwA6gyErKlPYmtzKFMw"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
app = FastAPI()

def fetch_data():
    response = supabase.table("all_events").select("*").execute()
    df = pd.DataFrame(response.data)
    df['Event_Date'] = pd.to_datetime(df['Event_Date'], errors='coerce')
    return df

def compute_score(row):
    try:
        CPA = float(row['CPA'])
        Fulfillment = float(row['Fulfillment_Percent'])
        Attendance = float(row['Attendance_Rate'])
        event_date = row['Event_Date']
        score = ((1 / CPA) * 0.5 + Fulfillment * 0.3 + Attendance * 0.2) * 40

        # Recency weighting
        days_ago = (datetime.datetime.now() - event_date).days
        if days_ago <= 30:
            score *= 1.25
        elif days_ago <= 90:
            score *= 1.0
        else:
            score *= 0.8
        return round(score, 2)
    except:
        return 0

@app.get("/vor")
def vor(topic: str, city: str, state: str, miles: Optional[int] = 6):
    df = fetch_data()

    df = df[df['Topic'].str.upper() == topic.upper()]
    df = df[df['City'].str.lower() == city.lower()]
    df = df[df['State'].str.lower() == state.lower()]

    if df.empty:
        return {"message": "No matching data found."}

    df['Score'] = df.apply(compute_score, axis=1)

    venue_summary = (
        df.groupby("Venue")
        .agg(
            Avg_CPA=('CPA', 'mean'),
            Avg_CPR=('CPR', 'mean'),
            Avg_Attendance=('Attendance_Rate', 'mean'),
            Avg_Fulfillment=('Fulfillment_Percent', 'mean'),
            Avg_Score=('Score', 'mean'),
            Event_Count=('Venue', 'count'),
            Last_Event=('Event_Date', 'max')
        )
        .sort_values(by="Avg_Score", ascending=False)
        .reset_index()
    )

    top_venues = venue_summary.head(4).to_dict(orient="records")
    return {"top_venues": top_venues}

@app.post("/score_manual")
def score_manual(CPA: float, Fulfillment_Percent: float, Attendance_Rate: float, Event_Date: str):
    try:
        event_date = pd.to_datetime(Event_Date)
        base_score = ((1 / CPA) * 0.5 + Fulfillment_Percent * 0.3 + Attendance_Rate * 0.2) * 40

        days_ago = (datetime.datetime.now() - event_date).days
        if days_ago <= 30:
            weighted_score = base_score * 1.25
        elif days_ago <= 90:
            weighted_score = base_score * 1.0
        else:
            weighted_score = base_score * 0.8

        return {"score": round(weighted_score, 2)}
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)








