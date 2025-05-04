from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

# Path to the locally hosted CSV
CSV_PATH = "data/AllEvents.csv"

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

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

@app.route("/vor", methods=["POST"])
def vor():
    try:
        payload = request.json
        topic = payload["topic"].strip().upper()
        city = payload["city"].strip().upper()
        state = payload["state"].strip().upper()

        # Topic normalization
        topic_map = {
            "TIR": "TAXES_IN_RETIREMENT_567",
            "EP": "ESTATE_PLANNING_567",
            "SS": "SOCIAL_SECURITY_567"
        }
        mapped_topic = topic_map.get(topic, topic)

        df = pd.read_csv(CSV_PATH)
        df.columns = [col.strip() for col in df.columns]

        # Filter by topic, city, state
        df = df[
            (df["Topic"].str.upper().str.strip() == mapped_topic) &
            (df["City"].str.upper().str.strip() == city) &
            (df["State"].str.upper().str.strip() == state)
        ].copy()

        if df.empty:
            return jsonify({"message": "No matching venues found"}), 404

        # Scoring fallback
        def safe_score(row):
            try:
                cpa = float(row["CPA"])
                fulfillment = float(row["Fulfillment_Percent"])
                attendance = float(row["Attendance_Rate"])
                return ((1 / cpa) * 0.5 + fulfillment * 0.3 + attendance * 0.2) * 40
            except:
                return 0

        df["CPA"] = pd.to_numeric(df["CPA"], errors="coerce")
        df["Fulfillment_Percent"] = pd.to_numeric(df["Fulfillment_Percent"], errors="coerce")
        df["Attendance_Rate"] = pd.to_numeric(df["Attendance_Rate"], errors="coerce")
        df["score"] = df.apply(safe_score, axis=1)

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
                "CPA": row.get("CPA", ""),
                "fulfillment_percent": row.get("Fulfillment_Percent", ""),
                "attendance_rate": row.get("Attendance_Rate", "")
            })

        return jsonify(result)

    except Exception as e:
        return jsonify({"error": "Failed to process VOR", "details": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)









