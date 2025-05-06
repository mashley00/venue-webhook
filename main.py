import pandas as pd
import datetime
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse
from supabase import create_client, Client
import os

app = FastAPI()

# Load env vars
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

print("✅ SUPABASE_URL raw:", repr(SUPABASE_URL))
print("✅ SUPABASE_KEY raw:", repr(SUPABASE_KEY[:10]) + "..." if SUPABASE_KEY else "❌ MISSING")
print("✅ Loading and preparing data from Supabase...")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

df = pd.DataFrame()

# Topic aliases
TOPIC_ALIASES = {
    "TIR": "taxes_in_retirement_567",
    "EP": "estate_planning_567",
    "SS": "social_security_567"
}

def normalize_topic(topic_abbr):
    return TOPIC_ALIASES.get(topic_abbr.upper(), topic_abbr)

def clean_column_names(df):
    df.columns = [col.strip().lower().replace(" ", "_").replace("-", "_") for col in df.columns]
    return df

def to_numeric(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def calculate_metrics(df):
    # Required fields
    required_cols = [
        "cpa", "cpr", "attended_hh", "gross_registrants", 
        "registration_max", "event_date"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Attendance Rate
    df["attendance_rate"] = df["attended_hh"] / df["gross_registrants"]
    df["fulfillment_pct"] = df["attended_hh"] / (df["registration_max"] / 2.4)

    # Score
    df["score"] = (1 / df["cpa"]) * 0.5 + df["fulfillment_pct"] * 0.3 + df["attendance_rate"] * 0.2

    # Weight by recency
    today = pd.to_datetime("today").normalize()
    df["event_date"] = pd.to_datetime(df["event_date"], errors='coerce')
    df["days_ago"] = (today - df["event_date"]).dt.days

    def weight_score(row):
        if pd.isna(row["score"]) or pd.isna(row["days_ago"]):
            return 0
        if row["days_ago"] <= 30:
            return row["score"] * 1.25
        elif row["days_ago"] <= 90:
            return row["score"] * 1.0
        else:
            return row["score"] * 0.8

    df["weighted_score"] = df.apply(weight_score, axis=1)
    df["score_out_of_40"] = (df["weighted_score"] * 40).round(1)
    return df

def format_results(filtered):
    from emoji import emojize

    output = []
    for _, row in filtered.iterrows():
        output.append(
            f""":first_place_medal: **{row['venue']}**
:round_pushpin: {row['city']}, {row['state']} ({row['distance'] or 'n/a'} miles)
:date: Most Recent Event: {row['event_date'].strftime('%Y-%m-%d')}
:spiral_calendar_pad: Number of events: {row['event_count']}
:chart_with_upwards_trend: Avg. Gross Registrants: {row['avg_registrants']}
:moneybag: Avg. CPA: ${row['cpa']:.2f}
:dollar: Avg. CPR: ${row['cpr']:.2f}
:chart_with_downwards_trend: Attendance Rate: {row['attendance_rate']:.1%}
:dart: Fulfillment %: {row['fulfillment_pct']:.1%}
:camera_with_flash: Image Allowed: {'✅' if row['venue_image_allowed'] else '❌'}
:warning: Disclosure Needed: {'✅' if row['venue_disclosure_needed'] else '❌'}
:sports_medal: Score: {row['score_out_of_40']}/40
:clock3: Best Time: 11:00am on Monday
"""
        )
    return "\n\n".join(output)

def get_top_venues(topic, city, state, miles=6):
    normalized_topic = normalize_topic(topic)
    subset = df.copy()

    subset = subset[
        (subset["topic"] == normalized_topic) &
        (subset["city"].str.lower() == city.lower()) &
        (subset["state"].str.lower() == state.lower())
    ]

    if subset.empty:
        return "❌ No matching venues found."

    grouped = subset.groupby("venue").agg({
        "event_date": "max",
        "venue_image_allowed": "last",
        "venue_disclosure_needed": "last",
        "gross_registrants": "mean",
        "cpa": "mean",
        "cpr": "mean",
        "attendance_rate": "mean",
        "fulfillment_pct": "mean",
        "weighted_score": "mean",
        "score_out_of_40": "mean"
    }).reset_index()

    grouped["event_count"] = subset.groupby("venue")["event_date"].count().values
    grouped["city"] = city
    grouped["state"] = state
    grouped["distance"] = miles
    grouped["avg_registrants"] = grouped["gross_registrants"].round(1)

    return format_results(grouped.sort_values("score_out_of_40", ascending=False).head(4))

@app.get("/", response_class=PlainTextResponse)
def home():
    return "Venue Option-INATOR is online."

@app.get("/vor", response_class=PlainTextResponse)
async def run_vor(request: Request):
    params = dict(request.query_params)
    try:
        topic = params.get("topic", "")
        city = params.get("city", "")
        state = params.get("state", "")
        miles = int(params.get("miles", 6))
        return get_top_venues(topic, city, state, miles)
    except Exception as e:
        return f"❌ Error: {str(e)}"

@app.on_event("startup")
def startup_event():
    load_and_prepare_data()

def load_and_prepare_data():
    global df
    response = supabase.table("All Events 1").select("*").execute()
    df_raw = pd.DataFrame(response.data)

    if df_raw.empty:
        raise Exception("⚠️ No data returned from Supabase.")

    df_raw = clean_column_names(df_raw)

    numeric_cols = [
        "cpa", "cpr", "attended_hh", "gross_registrants",
        "registration_max"
    ]
    df_raw = to_numeric(df_raw, numeric_cols)

    df_processed = calculate_metrics(df_raw)
    df = df_processed














