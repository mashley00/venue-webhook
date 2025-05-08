from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import logging

# ========== CONFIG ==========
app = FastAPI()
VERSION = "VOR v1.3.0 - S3 Load + POST JSON"
CSV_URL = "https://acquireup-venue-data.s3.us-east-2.amazonaws.com/all_events_23_25.csv"
logging.basicConfig(level=logging.INFO)

# ========== DATA MODEL ==========
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: int = 6

# ========== ROUTES ==========
@app.get("/")
def health_check():
    return {"status": "online", "version": VERSION}

@app.post("/vor")
def get_vor(request: VORRequest):
    try:
        logging.info(f"[{VERSION}] Loading dataset from: {CSV_URL}")
        df = pd.read_csv(CSV_URL)
        logging.info(f"✅ Loaded {len(df)} rows from CSV.")
    except Exception as e:
        logging.error(f"❌ Failed to load CSV from {CSV_URL}: {e}")
        raise HTTPException(status_code=500, detail=f"Error loading CSV file: {e}")

    # TEMP: Return confirmation only
    return {
        "message": "CSV loaded successfully.",
        "version": VERSION,
        "input_received": request.dict(),
        "row_count": len(df)
    }


















