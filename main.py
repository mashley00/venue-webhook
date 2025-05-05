from fastapi import FastAPI, Query
from supabase import create_client, Client
from typing import Optional
import pandas as pd
import datetime
import uvicorn

# Supabase config
SUPABASE_URL = "https://drcjaimdtalwvpvqbdmb.supabase.co"
SUPABASE_KEY = "YOUR_SUPABASE_KEY"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()

# Topic normalization
topic_map = {
    "SS": "Social_Security_567",
    "TIR": "Taxes_in_Retirement_567",
    "EP": "Estate_Planning_567"
}

def fetch_data():
    response = supabase.table("all_events").select("*").execute()
    df = pd.DataFrame(response.data)
    df.rename(columns={
        'Event Date': 'Event_Date',
        'Venue Image Allowed (Time of Event)': 'Venue_Image_Allowed',
        'Venue Disclosure Needed': 'Venue_Disclosure_Needed',
        'FB CPR': 'CPR',
        'Cost per Verified HH': 'CPA',
        'Gross Registrants': 'Gross_Registrants',
        'Attended HH': 'Attended_HH',
        'Registration Max': 'Registration_Max'
    }, inplace=True)
    df['Event_Date'] = pd.to_datetime(df['Event_Date'], errors='coerce')
    return df

def compute_score(row):
    try:
        CPA = row['CPA']
        Fulfillment = row['Fulfillment_Percent']
        Attendance = row['Attendance_Rate']
        score = ((1 / CPA) * 0.5 + Fulfillment * 0.3 + Attendance * 0.2) * 40
        days_ago = (datetime.datetime.now() - row['Event_Date']).days
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
    mapped_topic = topic_map.get(topic.upper(), topic)
    df = df[
        (df['Topic'] == mapped_topic) &
        (df['City'].str.lower() == city.lower()) &
        (df['State'].str.lower() == state.lower())
    ]
    if df.empty:
        return {"message": "No matching data found."}

    df['Attendance_Rate'] = df['Attended_HH'] / df['Gross_Registrants']
    df['Fulfillment_Percent'] = df['Attended_HH'] / (df['Registration_Max'] / 2.4)
    df['Score'] = df.apply(compute_score, axis=1)

    summary = df.groupby("Venue").agg(
        Avg_CPA=('CPA', 'mean'),
        Avg_CPR=('CPR', 'mean'),
        Avg_Attendance=('Attendance_Rate', 'mean'),
        Avg_Fulfillment=('Fulfillment_Percent', 'mean'),
        Avg_Score=('Score', 'mean'),
        Event_Count=('Venue', 'count'),
        Last_Event=('Event_Date', 'max'),
        Image_Allowed=('Venue_Image_Allowed', 'max'),
        Disclosure_Required=('Venue_Disclosure_Needed', 'max')
    ).sort_values(by="Avg_Score", ascending=False).reset_index()

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
    mapped_topic = topic_map.get(topic.upper(), topic)
    df = df[
        (df['Venue'].str.lower() == venue.lower()) &
        (df['Topic'] == mapped_topic)
    ]
    if df.empty:
        return {"message": "No data for this venue/topic combination."}

    last_event = df['Event_Date'].max()
    days_since = (datetime.datetime.now() - last_event).days
    decay = 0.6 if days_since < 30 else 0.75 if days_since < 60 else 0.9 if days_since < 90 else 1.0

    return {
        "Projected_CPA": round(df['CPA'].mean() / decay, 2),
        "Projected_Registrants": int(df['Gross_Registrants'].mean() * decay),
        "Projected_Attendance_Rate": round(df['Attendance_Rate'].mean(), 2),
        "Projected_Fulfillment": round(df['Fulfillment_Percent'].mean(), 2),
        "Days_Since_Last_Event": days_since
    }

@app.get("/recommend_schedule")
def recommend_schedule(city: str, topic: str):
    df = fetch_data()
    mapped_topic = topic_map.get(topic.upper(), topic)
    df = df[
        (df['City'].str.lower() == city.lower()) &
        (df['Topic'] == mapped_topic)
    ]
    if df.empty:
        return {"message": "No matching data found."}

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

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)









