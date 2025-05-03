from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vR_ohhyjy3dRXiuMUHzIs4Uww1AdkXfwIEBBDjnh57povZyLs6F0aXyLAI-1QkhUcyASUPfAkyl4H9K/pub?gid=0&single=true&output=csv"

TOPIC_MAP = {
    "TIR": "taxes_in_retirement_567",
    "SS": "social_security_567",
    "EP": "estate_planning_567"
}

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

@app.route("/vor", methods=["POST"])
def vor():
    try:
        payload = request.json
        topic_code = payload["topic"].strip().upper()
        city = payload["city"].strip().lower()
        state = payload["state"].strip().lower()
        topic_value = TOPIC_MAP.get(topic_code)

        if not topic_value:
            return jsonify({"error": f"Invalid topic code '{topic_code}'"}), 400

        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # Ensure required columns exist
        required = {"cpa", "attendance_rate", "fulfillment_percent", "topic", "city", "state"}
        if not required.issubset(set(df.columns)):
            return jsonify({"error": "Missing required columns", "details": list(df.columns)}), 500

        # Filter by city/state/topic
        df = df[
            (df["topic"].str.strip().str.lower() == topic_value.lower()) &
            (df["city"].str.strip().str.lower() == city) &
            (df["state"].str.strip().str.lower() == state)
        ].copy()

        if df.empty:
            return jsonify({"message": "No matching venues found"}), 404

        # Try calculating fulfillment if missing
        if df["fulfillment_percent"].isnull().all() and "attended_hh" in df and "registration_max" in df:
            df["fulfillment_percent"] = df["attended_hh"] / (df["registration_max"] / 2.4)

        # Numeric cleanup
        df["cpa"] = pd.to_numeric(df["cpa"], errors="coerce")
        df["attendance_rate"] = pd.to_numeric(df["attendance_rate"], errors="coerce")
        df["fulfillment_percent"] = pd.to_numeric(df["fulfillment_percent"], errors="coerce")

        df = df.dropna(subset=["cpa", "attendance_rate", "fulfillment_percent"])

        # Scoring logic
        df["score"] = (1 / df["cpa"]) * 0.5 + df["fulfillment_percent"] * 0.3 + df["attendance_rate"] * 0.2
        df["score"] = df["score"] * 40

        top_venues = df.sort_values("score", ascending=False).head(4)

        result = []
        for _, row in top_venues.iterrows():
            result.append({
                "venue": row.get("venue", ""),
                "score": round(row["score"], 2),
                "recommended_time_1": "11:00 AM Monday",
                "recommended_time_2": "6:30 PM Tuesday",
                "event_date": row.get("event_date", ""),
                "event_time": row.get("event_time", ""),
                "job_number": row.get("job_number", ""),
                "CPA": row.get("cpa", ""),
                "fulfillment_percent": f"{round(row['fulfillment_percent'], 2)}%",
                "attendance_rate": f"{round(row['attendance_rate'], 2)}%",
                "image_allowed": row.get("venue_image_allowed_(current)", ""),
                "disclosure_needed": row.get("venue_disclosure_needed", "")
            })

        return jsonify(result)

    except Exception as e:
        return jsonify({
            "error": "Failed to process VOR",
            "details": str(e)
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)









