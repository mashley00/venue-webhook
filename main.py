from fastapi import FastAPI, Request
from pydantic import BaseModel
import pandas as pd
from typing import Optional

app = FastAPI()

# Define your S3 public CSV URL
CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"

# Define the request model
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: Optional[int] = 6

@app.post("/vor")
async def venue_optimization(request: VORRequest):
    try:
        # Load data from S3
        df = pd.read_csv(CSV_URL)

        # Placeholder logic to confirm the app is working
        filtered = df[
            (df['Topic'].str.upper() == request.topic.upper()) &
            (df['City'].str.upper() == request.city.upper()) &
            (df['State'].str.upper() == request.state.upper())
        ]

        count = len(filtered)

        return {
            "message": "Data processed successfully",
            "rows_matching": count,
            "sample_venues": filtered['Venue'].dropna().unique().tolist()[:5]
        }

    except Exception as e:
        # Log the error server-side (optional enhancement: log to a monitoring tool)
        print("❌ Error occurred:", str(e))

        # Return diagnostic info
        return {
            "error": "Internal server error",
            "details": str(e)
        }















