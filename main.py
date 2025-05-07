from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import datetime
import traceback

app = FastAPI()

# Pydantic model to parse input
class VorRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: int = 6  # default value

@app.post("/vor")
def venue_optimization(request: VorRequest):
    try:
        # --- Step 1: Load data ---
        url = "https://raw.githubusercontent.com/mashley00/VenueGPT/refs/heads/main/All%20Events%2023%20to%2025%20TIR%20EP%20SS%20CSV%20UTF%208.csv"
        df = pd.read_csv(url, encoding="utf-8")

        # --- Step 2: Filter based on input ---
        topic = request.topic.strip().upper()
        city = request.city.strip().title()
        state = request.state.strip().upper()

        df = df[df["Topic"].str.upper() == topic]
        df = df[df["City"].str.title() == city]
        df = df[df["State"].str.upper() == state]

        if df.empty:
            raise HTTPException(status_code=404, detail="No events found for that topic and location.")

        # --- Step 3: Process results ---
        # Example: just count matching events and return basic info
        result = {
            "matching_events": len(df),
            "avg_registrants": round(df["Gross Registrants"].mean(), 2),
            "avg_attended": round(df["Attended HH"].mean(), 2),
            "avg_cpr": round(df["FB CPR"].mean(), 2),
            "avg_cpa": round(df["Cost per Verified HH"].mean(), 2)
        }

        return result

    except Exception as e:
        # Logs the full traceback to the server logs and returns the error to client
        print("ERROR during /vor request:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
















