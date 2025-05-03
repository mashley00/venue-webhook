from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

# ✅ Google Sheets CSV Export Link (TIR/EP/SS Master Sheet)
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQFeUqc0rsZVRzVk6vmL0ObtedPUojk1KFaa62o0VhF--7PqWp9c8sEqNC0pPyM89DFNUPVQ_yBwG-H/pub?gid=865531721&single=true&output=csv"

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

        # ✅ Load directly from Google Sheets CSV export
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip() for col in df.columns]

        df = df[
            (df['Topic'].str.upper().str.strip() == topic


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

