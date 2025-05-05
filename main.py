
from flask import Flask, request, jsonify
import pandas as pd
import requests
from io import StringIO

app = Flask(__name__)

# GitHub-hosted CSV URL
CSV_URL = "https://raw.githubusercontent.com/mashley00/venue-webhook/main/data/AllEvents.csv"

# Health check route
@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

# Manual scoring endpoint (Zapier)
@app.route("/score_manual", methods=["POST"])
def score_manual():
    data = request.json
    try:
        cpa = float(data["CPA"])
        fulfillment = float(data["Fulfillment_Percent"].replace("%", "").strip())
        attendance = float(data["Attendance_Rate"].replace("%", "").strip())
        raw_score = (1 / cpa) * 0.5 + fulfillment * 0.3 + attendance * 0.2
        normalized_score = min((raw_score / 2.5) * 100, 100)
    except Exception as e:
        return jsonify({"error": "Invalid input", "details": str(e)}), 400

    return jsonify({
        "venue": data.get("Venue", "Unknown"),
        "score": round(normalized_score, 2),
        "recommended_time_1": "11:00 AM Monday",
        "recommended_time_2": "6:30 PM Tuesday"
    })

# VOR endpoint for optimized venue suggestions
@app.route("/vor", methods=["POST"])
def vor():
    try:
        payload = request.json
        topic = payload["topic"].strip().upper()
        city = payload["city"].strip().upper()
        state = payload["state"].strip().upper()

        topic_map = {
            "TIR": "TAXES_IN_RETIREMENT_567",
            "EP": "ESTATE_PLANNING_567",
            "SS": "SOCIAL_SECURITY_567"
        }
        mapped_topic = topic_map.get(topic, topic)

        response = requests.get(CSV_URL)
        if response.status_code != 200:
            return jsonify({"error": "Failed to fetch dataset from GitHub"}), 500

        df = pd.read_csv(StringIO(response.text))
        df.columns = [col.strip() for col in df.columns]

        df = df[
            (df["Topic"].str.upper().str.strip() == mapped_topic) &
            (df["City"].str.upper().str.strip() == city) &
            (df["State"].str.upper().str.strip() == state)
        ].copy()

        if df.empty:
            return jsonify({"message": f"No matching venues found for {topic} in {city}, {state}."}), 404

        df["CPA"] = pd.to_numeric(df["CPA"], errors="coerce")
        df["Fulfillment_Percent"] = pd.to_numeric(df["Fulfillment_Percent"], errors="coerce")
        df["Attendance_Rate"] = pd.to_numeric(df["Attendance_Rate"], errors="coerce")

        def calculate_score(row):
            try:
                raw = (1 / row["CPA"]) * 0.5 + row["Fulfillment_Percent"] * 0.3 + row["Attendance_Rate"] * 0.2
                return min((raw / 2.5) * 100, 100)
            except:
                return 0

        df["score"] = df.apply(calculate_score, axis=1)
        df = df.sort_values("score", ascending=False).head(4)

        venues = []
        for _, row in df.iterrows():
            venues.append({
                "🥇 Venue Name": row.get("Venue", ""),
                "📍 Location": f"{row.get('City', '')}, {row.get('State', '')}",
                "🗓️ Most Recent Event": row.get("Event_Date", ""),
                "📆 Total Events": df[df["Venue"] == row["Venue"]].shape[0],
                "👥 Avg. Gross Registrants": round(df[df["Venue"] == row["Venue"]]["Gross_Registrants"].mean(), 2),
                "💰 Avg. CPA": f"${round(row.get('CPA', 0), 2)}",
                "📈 FB CPR": f"${round(row.get('FB_CPR', 0), 2)}",
                "🎯 Attendance Rate": f"{round(row.get('Attendance_Rate', 0), 2)}%",
                "📊 Fulfillment %": f"{round(row.get('Fulfillment_Percent', 0), 2)}%",
                "🖼️ Image Allowed": "✅" if str(row.get("Venue_Image_Allowed-Current", "")).strip().lower() == "yes" else "❌",
                "⚠️ Disclosure Needed": "✅" if str(row.get("Venue_Disclosure_Needed", "")).strip().lower() == "yes" else "❌",
                "🏆 Score": f"{round(row.get('score', 0), 2)} / 100",
                "🕓 Best Time": "11:00 AM on Monday"
            })

        return jsonify(venues), 200

    except Exception as e:
        return jsonify({"error": "Failed to process VOR", "details": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)








