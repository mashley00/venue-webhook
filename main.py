from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

# Use the new Google Sheets CSV export link
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vR_ohhyjy3dRXiuMUHzIs4Uww1AdkXfwIEBBDjnh57povZyLs6F0aXyLAI-1QkhUcyASUPfAkyl4H9K/pub?gid=0&single=true&output=csv"

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

@app.route("/analyze_venue", methods=["POST"])
def analyze_venue():
    data = request.json

    try:
        cpa = float(data["CPA"])
        fulfillment = float(data["Fulfillment_Percent"].replace("%", "").strip())
        attendance = float(data["Attendance_Rate"].replace("%", "").strip())
        score = (1 / cpa) * 0.5 + fulfillment * 0.3 + attendance * 0.2
        score *= 40
    except Exception as e:
        return jsonify({"error": "Invalid input", "details": str(e)}), 400

    return jsonify({
        "venue": data.get("Venue", "Unknown"),
        "score": round(score, 2),
        "recommended_time_1": "11:00 AM Monday",
        "recommended_time_2": "6:30 PM Tuesday"
    })

@app.route("/vor", methods=["POST"])
def vor():
    try:
        payload = request.json
        topic = payload["topic"].strip().upper()
        city = payload["city"].strip().upper()
        state = payload["state"].strip().upper()
        radius = float(payload["radius"])

        # Load CSV from Google Sheets
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip() for col in df.columns]

        # Filter for topic, city, state, and radius
        df = df[
            (df['Topic'].str.upper().str.strip() == topic) &
            (df['City'].str.upper().str.strip() == city) &
            (df['State'].str.upper().str.strip() == state) &
            (df['Miles from Center'] <= radius)
        ].copy()

        if df.empty:
            return jsonify({"message": "No matching venues found"}), 404

        # Clean and calculate score
        df["CPA_float"] = pd.to_numeric(df["Cost per Verified HH"], errors="coerce")
        df["Fulfillment"] = pd.to_numeric(df["Fulfillment %"], errors="coerce")
        df["Attendance"] = pd.to_numeric(df["Attendance Rate"], errors="coerce")
        df = df.dropna(subset=["CPA_float", "Fulfillment", "Attendance"])

        df["score"] = (1 / df["CPA_float"]) * 0.5 + df["Fulfillment"] * 0.3 + df["Attendance"] * 0.2
        df["score"] = df["score"] * 40

        top_venues = df.sort_values("score", ascending=False).head(4)

        result = []
        for _, row in top_venues.iterrows():
            result.append({
                "venue": row.get("Venue", ""),
                "score": round(row["score"], 2),
                "recommended_time_1": "11:00 AM Monday",
                "recommended_time_2": "6:30 PM Tuesday",
                "event_date": row.get("Event Date", ""),
                "event_time": row.get("Event Time", ""),
                "job_number": row.get("Job Number", ""),
                "CPA": row.get("Cost per Verified HH", ""),
                "fulfillment_percent": row.get("Fulfillment %", ""),
                "attendance_rate": row.get("Attendance Rate", "")
            })

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": "Failed to process VOR", "details": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)




