from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
import logging
import traceback

app = FastAPI()

# Set up logging
logging.basicConfig(level=logging.DEBUG)

# Exception handler to log uncaught errors
@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logging.error("Unhandled exception occurred:", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal Server Error", "details": str(exc)}
    )

# Define Pydantic model for POST request body
class VORRequest(BaseModel):
    topic: str
    city: str
    state: str
    miles: int = 6

@app.post("/vor")
async def get_vor(request: Request):
    try:
        # Load data
        df = pd.read_csv("https://raw.githubusercontent.com/mashley00/VenueGPT/refs/heads/main/All%20Events%2023%20to%2025%20TIR%20EP%20SS%20CSV%20UTF%208.csv")

        # Parse request JSON
        data = await request.json()
        logging.debug(f"Received request data: {data}")

        # Simple response to confirm working endpoint (replace with actual logic)
        return {
            "message": "Request received",
            "parsed_data": data,
            "row_count": len(df)
        }

    except Exception as e:
        logging.error("Exception in /vor handler:", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "Internal Server Error", "details": str(e)}
        )

















