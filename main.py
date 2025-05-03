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
        df.columns = [col.strip().lower() for col in df.columns]

        # Safe renaming for fuzzy matches
        rename_map = {}
        for col in df.columns:
            if "cost per verified hh" in col:
                rename_map[col] = "cpa"
            elif "fulfillment" in col:
                rename_map[col] = "fulfillment"
            elif "attendance rate" in col:
                rename_map[col] = "attendance"
            elif "attended hh" in col:
                rename_map[col] = "attended"
            elif "registration max" in col:
                rename_map[col] = "reg_max"
            elif "venue image" in col:
                rename_map[col] = "image_allowed"
            elif "disclosure" in col:
                rename_map[col] = "disclosure_needed"

        df.rename(columns=rename_map, inplace=True)

        # Filter
        df = df[
            (df["topic"].str.strip().str.lower() == topic_value.lower()) &
            (df["city"].str.strip().str.lower() == city) &
            (df["state"].str.strip().str.lower() == state)
        ].copy()

        if df.empty:
            return jsonify({"message": "No matching venues found"}), 404

        # Derive Fulfillment % if needed
        if "fulfillment" not in df or df["fulfillment"].isnull().all():
            if "attended" in df and "reg_max" in df:
                df["fulfillment"] = df["attended"] / (df["reg_max"] / 2.4)

        # Clean & convert
        df["fulfillment"] = df["fulfillment"].astype(str).str.replace("%", "").astype(float)
        df["attendance"] = df["attendance"].astype(str).str.replace("%", "").astype(float)
        df["cpa"] = pd.to_numeric(df["cpa"], errors="coerce")

        df = df.dropna(subset=["cpa", "fulfillment", "attendance"])

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
        return jsonify({
            "error": "Failed to process VOR",
            "details": str(e),
            "columns": list(df.columns) if 'df' in locals() else "Dataframe not loaded"
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)








