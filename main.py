from fastapi import FastAPI
from supabase import create_client, Client
import pandas as pd
from typing import Optional
import datetime
import os
from dotenv import load_dotenv
import uvicorn

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ SUPABASE_URL or SUPABASE_KEY not set!")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
app = FastAPI()

# Topic mapping
topic_map = {
    "TIR": "Taxes_in_Retirement_567",
    "EP": "Estate_Planning_567",
    "SS": "Social_Security_567"
}

# Column helper
def get_column(df, preferred, fallback_list):
    for col in [preferred] + fallback_list:
        if col in df.columns:
            return col
    return None

def fetch_data():
    response = supabase.table("All Events 1").select("*").execute()
    df = pd.DataFrame(response.data)

    df.columns = df.columns.str.replace(" ", "_").str.strip()
    df['Event_Date'] = pd.to_datetime(df['Event_Date'], errors='coerce')
    return df

def compute_score(row):
    try:
        CPA = float(row['CPA'])
        Fulfillment = row['Fulfillment_Percent']
        Attendance = row['Attendance_Rate']
        score = ((1 / CPA) * 0.5 + Fulfillment * 0.3 + Attendance * 0.2) * 40
        days_ago = (datetime.datetime.now() - row['Event_Date']).days
        score *= 1.25 if days_ago <= 30 else 1.0 if days_ago <= 90 else 0.8
        return round(score, 2)
    except:
        return 0

@app.get("/vor")
def vor(topic: str, city: str, state: str, miles: Optional[int] = 6):
    df = fetch_data()
    topic_value = topic_map.get(topic.upper(), topic)
    df = df[
        (df['Topic'] == topic_value) &
        (df['City'].str.lower() == city.lower()) &
        (df['State'].str.upper() == state.upper())
    ]

    if df.empty:
        return {"message": f"No matching data found for topic '{topic}' in {city}, {state}."}

    cpr_col = get_column(df, 'CPR', ['FB_CPR'])
    img_col = get_column(df, 'Venue_Image_Allowed', ['Venue_Image_Allowed_Current', 'Venue_Image_Allowed-Current'])
    disclosure_col = get_column(df, 'Venue_Disclosure_Needed', [])

    df['Attendance_Rate'] = df['Attended_HH'] / df['Gross_Registrants']
    df['Fulfillment_Percent'] = df['Attended_HH'] / (df['Registration_Max'] / 2.4)
    df['Score'] = df.apply(compute_score, axis=1)

    agg_dict = {
        'CPA': ('CPA', 'mean'),
        'Attendance_Rate': ('Attendance_Rate', 'mean'),
        'Fulfillment_Percent': ('Fulfillment_Percent', 'mean'),
        'Score': ('Score', 'mean'),
        'Venue': ('Venue', 'count'),
        'Event_Date': ('Event_Date', 'max'),
    }

    if cpr_col:
        agg_dict['CPR'] = (cpr_col, 'mean')
    if img_col:
        agg_dict['Image_Allowed'] = (img_col, 'max')
    if disclosure_col:
        agg_dict['Disclosure_Required'] = (disclosure_col, 'max')

    summary = (
        df.groupby("Venue")
        .agg(**agg_dict)
        .rename(columns={
            'Venue': 'Event_Count',
            'Event_Date': 'Last_Event'
        })
        .sort_values(by="Score", ascending=False)
        .reset_index()
    )

    return summary.head(4).to_dict(orient="records")

@app.post("/score_manual")
def score_manual(CPA: float, Fulfillment_Percent: float, Attendance_Rate: float, Event_Date: str):
    event_date = pd.to_datetime(Event_Date)
    base_score = ((1 / CPA) * 0.5 + Fulfillment_Percent * 0.3 + Attendance_Rate * 0.2) * 40
    days_ago = (datetime.datetime.now() - event_date).days
    weight = 1.25 if days_ago <= 30 else 1.0 if days_ago <= 90 else 0.8
    return {"score": round(base_score * weight, 2)}

@app.get("/predict_venue")
def predict_venue(venue: str, topic: str):
    df = fetch_data()
    topic_value = topic_map.get(topic.upper(), topic)
    df = df[
        (df['Venue'].str.lower() == venue.lower()) &
        (df['Topic'] == topic_value)
    ]
    if df.empty:
        return {"message": f"No historical data found for {venue} and topic {topic}"}

    last_event = df['Event_Date'].max()
    days_since = (datetime.datetime.now() - last_event).days
    decay = 0.6 if days_since < 30 else 0.75 if days_since < 60 else 0.9 if days_since < 90 else 1.0

    return {
        "Projected_CPA": round(df['CPA'].mean() / decay, 2),
        "Projected_Registrants": int(df['Gross_Registrants'].mean() * decay),
        "Projected_Attendance_Rate": round(df['Attended_HH'].sum() / df['Gross_Registrants'].sum(), 2),
        "Projected_Fulfillment": round(df['Attended_HH'].sum() / (df['Registration_Max'].sum() / 2.4), 2),
        "Days_Since_Last_Event": days_since
    }

@app.get("/recommend_schedule")
def recommend_schedule(city: str, topic: str):
    df = fetch_data()
    topic_value = topic_map.get(topic.upper(), topic)
    df = df[
        (df['City'].str.lower() == city.lower()) &
        (df['Topic'] == topic_value)
    ]
    if df.empty:
        return {"message": "No matching data found for that city/topic."}

    df['Day'] = df['Event_Date'].dt.day_name()
    df['Time'] = df['Event_Date'].dt.strftime('%H:%M')
    df['Attendance_Rate'] = df['Attended_HH'] / df['Gross_Registrants']
    df['Fulfillment_Percent'] = df['Attended_HH'] / (df['Registration_Max'] / 2.4)

    schedule_perf = df.groupby(['Day', 'Time']).agg(
        CPA=('CPA', 'mean'),
        Fulfillment=('Fulfillment_Percent', 'mean'),
        Attendance=('Attendance_Rate', 'mean'),
        Count=('Venue', 'count')
    ).reset_index()

    schedule_perf['Score'] = (1 / schedule_perf['CPA']) * 0.5 + \
                             schedule_perf['Fulfillment'] * 0.3 + \
                             schedule_perf['Attendance'] * 0.2

    return schedule_perf.sort_values(by='Score', ascending=False).head(5).to_dict(orient='records')

# Local test runner (only if run manually)
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)












