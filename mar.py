import pandas as pd
from datetime import datetime
from typing import Optional

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace(r"[^\w\s]", "", regex=True)
    df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
    df['market'] = df['city'].str.strip() + ', ' + df['state'].str.strip()
    df['attendance_rate'] = df['attended_hh'] / df['gross_registrants']
    df['fulfillment'] = df['attended_hh'] / (df['registration_max'] / 2.4)
    return df

def estimate_decay_cpr(base_cpr: float, days_since_last: Optional[int], slope: float = -0.014) -> float:
    if days_since_last is not None and pd.notna(base_cpr):
        return base_cpr + (slope * days_since_last)
    return base_cpr

def calculate_media_overlay(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    df = df.copy()
    df['frequency'] = df['fb_impressions'] / df['fb_reach']
    df['registrants_per_1k'] = df['gross_registrants'] / (df['fb_impressions'] / 1000)
    df['estimated_cvr'] = df['gross_registrants'] / df['fb_impressions']
    df['estimated_cpr'] = df['cpm'] / df['registrants_per_1k']
    return {
        "avg_cpm": round(df['cpm'].mean(), 2),
        "estimated_cvr": round(df['estimated_cvr'].mean(), 4),
        "registrants_per_1k": round(df['registrants_per_1k'].mean(), 2),
        "estimated_media_cpr": round(df['estimated_cpr'].mean(), 2),
        "avg_frequency": round(df['frequency'].mean(), 2)
    }

def generate_mar(df: pd.DataFrame, topic: str, city: str, state: str, event_date_str: Optional[str] = None) -> dict:
    df = clean_columns(df)
    topic_code = topic.upper()
    event_date = pd.to_datetime(event_date_str) if event_date_str else pd.Timestamp.today()

    filtered = df[
        (df['topic'] == topic_code) &
        (df['city'].str.lower().str.strip() == city.lower().strip()) &
        (df['state'].str.upper().str.strip() == state.upper().strip())
    ]

    if filtered.empty:
        return {"error": "No matching events found for this topic and market."}

    venue = filtered['venue'].mode().iloc[0]
    venue_events = filtered[filtered['venue'] == venue]

    last_event_date = venue_events['event_date'].max()
    days_since_venue = (event_date - last_event_date).days if pd.notna(last_event_date) else None

    venue_avg = venue_events[['gross_registrants', 'cost_per_verified_hh']].mean()
    predicted_regs = venue_avg['gross_registrants']
    predicted_cpr = estimate_decay_cpr(venue_avg['cost_per_verified_hh'], days_since_venue)

    media_df = venue_events.dropna(subset=['fb_impressions', 'fb_reach', 'cpm', 'gross_registrants'])
    media_overlay = calculate_media_overlay(media_df)

    return {
        "venue": venue,
        "market": f"{city.title()}, {state.upper()}",
        "topic": topic_code,
        "event_date": event_date.date().isoformat(),
        "days_since_last_venue_use": days_since_venue,
        "predicted_registrants": round(predicted_regs, 1) if pd.notna(predicted_regs) else None,
        "predicted_cpr": round(predicted_cpr, 2) if pd.notna(predicted_cpr) else None,
        "media_overlay": media_overlay
    }
