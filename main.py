from flask import Flask, request, jsonify
import pandas as pd
import requests
from io import StringIO

app = Flask(__name__)

# Always-on GitHub-hosted CSV
CSV_URL = "https://raw.githubusercontent.com/mashley00/venue-webhook/main/data/AllEvents.csv"

# Health check route
@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

# Manual scoring endpoint (for Zapier or manual forms)
@app.route("/score_manual", methods=["POST"])
def score_manual():
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

# Venue Optimization Request endpoint
@app.route("/vor", methods=["POST"])
def vor():
    try:
        payload = request.json
        topic = payload["topic"].strip().upper()
        city = payload["city"].strip().upper()
        state = payload["state"].strip().upper()

        # Normalize topic
        topic_map = {
            "TIR": "TAXES_IN_RETIREMENT_567",
            "EP": "ESTATE_PLANNING_567",
            "SS": "SOCIAL_SECURITY_567"
        }
        mapped_topic = topic_map.get(topic, topic)

        # Load data from GitHub
        response = requests.get(CSV_URL)
        if response.status_code != 200:
            return jsonify({"error": "Failed to load dataset from GitHub"}), 500

        df = pd.read_csv(StringIO(response.text))
        df.columns = [col.strip() for col in df.columns]

        # Ensure required column is present
        if "Gross_Registrants" not in df.columns:
            return jsonify({"error": "Missing column: Gross_Registrants"}), 500

        # Filter matching rows
        df = df[
            (df["Topic"].str.upper().str.strip() == mapped_topic) &
            (df["City"].str.upper().str.strip() == city) &
            (df["State"].str.upper().str.strip() == state)
        ].copy()

        if df.empty:
            return jsonify({"message": f"No matching venues found for {topic} in {city}, {state}."}), 404

        # Prepare for scoring
        df["CPA"] = pd.to_numeric(df["CPA"], errors="coerce")
        df["Fulfillment_Percent"] = pd.to_numeric(df["Fulfillment_Percent"], errors="coerce")
        df["Attendance_Rate"] = pd.to_numeric(df["Attendance_Rate"], errors="coerce")

        def calculate_score(row):
            try:
                return ((1 / row["CPA"]) * 0.5 + row["Fulfillment_Percent"] * 0.3 + row["Attendance_Rate"] * 0.2) * 40
            except:
                return 0

        df["score"] = df.apply(calculate_score, axis=1)
        top_venues = df.sort_values("score", ascending=False).head(4)

        result = []
        for _, row in top_venues.iterrows():
            venue_subset = df[df["Venue"] == row["Venue"]]
            result.append({
                "🥇 Venue Name": row.get("Venue", ""),
                "📍 Location": f"{row.get('City', '')}, {row.get('State', '')}",
                "🗓️ Most Recent Event": row.get("Event Date", ""),
                "📆 Total Events": venue_subset.shape[0],
                "👥 Avg. Gross Registrants": round(venue_subset["Gross_Registrants"].mean(), 2),
                "💰 Avg. CPA": f"${round(row.get('CPA', 0), 2)}",
                "📈 FB CPR": f"${round(row.get('FB CPR', 0), 2)}",
                "🎯 Attendance Rate": f"{round(row.get('Attendance_Rate', 0), 2)}%",
                "📊 Fulfillment %": f"{round(row.get('Fulfillment_Percent', 0), 2)}%",
                "🖼️ Image Allowed": "✅" if str(row.get("Venue Image Allowed (Current)", "")).strip().lower() == "yes" else "❌",
                "⚠️ Disclosure Needed": "✅" if str(row.get("Venue Disclosure Needed", "")).strip().lower() == "yes" else "❌",
                "🏆 Score": f"{round(row.get('score', 0), 2)} / 40",
                "🕓 Best Time": "11:00 AM on Monday"
            })

        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": "Failed to process VOR", "details": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)








