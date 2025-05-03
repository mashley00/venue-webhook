from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

# ✅ Live Google Sheets CSV export URL
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vR_ohhyjy3dRXiuMUHzIs4Uww1AdkXfwIEBBDjnh57povZyLs6F0aXyLAI-1QkhUcyASUPfAkyl4H9K/pub?gid=0&single=true&output=csv"

# ✅ Topic mapping
TOPIC_MAP = {
    "TIR": "taxes_in_retirement_567",
    "SS": "social_security_567",
    "EP": "estate_planning_567"
}

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

        topic_value = TOPIC_MAP.get(topic)
        if not topic_value:
            return jsonify({"error": f"Invalid topic code: {topic}"}), 400

        # Load and clean data
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().lower() for col in df.columns]

        # Rename for uniformity
        df.rename(columns={
            "cost per verified hh": "cpa",
            "fulfillment %": "fulfillment",
            "attendance rate": "attendance",
            "attended hh": "attended",
            "registration max": "reg_max",
            "venue image allowed": "image_allowed",
            "disclosure required": "disclosure_needed"
        }, inplace=True)

        # Filter
        df = df[
            (df["topic"].str.lower().str.strip() == topic_value.lower()) &
            (df["city"].str.lower().str.strip() == city.lower()) &
            (df["state"].str.lower().str.strip() == state.lower())
        ].copy()

        if df.empty:
            return jsonify({"message": "No matching venues found"}), 404

        # Calculate Fulfillment % if not present
        if "fulfillment" not in df or df["fulfillment"].isnull().all():
            if "attended" in df and "reg_max" in df:
                df["fulfillment"] = df["attended"] / (df["reg_max"] / 2.4)

        # Clean % strings
        df["fulfillment"] = df["fulfillment"].astype(str).str.replace("%", "").astype(float)
        df["attendance"] = df["attendance"].astype(str).str.replace("%", "").astype(float)
        df["cpa"] = pd.to_numeric(df["cpa"], errors="coerce")

        df = df.dropna(subset=["cpa", "fulfillment", "attendance"])

        # Score
        df["score"] = (1 / df["cpa"]) * 0.5 + df["fulfillment"] * 0.3 + df["attendance"] * 0.2
        df["score"] = df["score"] * 40

        top_venues = df.sort_values("score", ascending=False).head(4)

        result = []
        for _, row in top_venues.iterrows():
            result.append({
                "venue": row.get("venue", ""),
                "score": round(row["score"], 2),
                "recommended_time_1": "11:00 AM Monday",
                "recommended_time_2": "6:30 PM Tuesday",
                "event_date": row.get("event date", ""),
                "event_time": row.get("event time", ""),
                "job_number": row.get("job number", ""),
                "CPA": row.get("cpa", ""),
                "fulfillment_percent": f"{round(row['fulfillment'], 2)}%",
                "attendance_rate": f"{round(row['attendance'], 2)}%",
                "image_allowed": row.get("image_allowed", ""),
                "disclosure_needed": row.get("disclosure_needed", "")
            })

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": "Failed to process VOR", "details": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)







