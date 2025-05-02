from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200

@app.route("/analyze_venue", methods=["POST"])
def analyze_venue():
    data = request.json

    try:
        # Convert core fields
        cpa = float(data["CPA"])
        fulfillment = float(data["Fulfillment_Percent"].replace("%", "").strip())
        attendance = float(data["Attendance_Rate"].replace("%", "").strip())

        # Optional metadata fields
        venue = data.get("Venue", "Unknown Venue")
        topic = data.get("Topic", "Unknown Topic")
        job_number = data.get("Job_Number", "")
        event_date = data.get("Event_Date", "")
        event_time = data.get("Event_Time", "")
        image_allowed = data.get("Image_Allowed", False)
        disclosure_needed = data.get("Disclosure_Needed", False)
        city = data.get("City", "")
        state = data.get("State", "")
        gross_registrants = data.get("Gross_Registrants", None)
        attended_hh = data.get("Attended_HH", None)

        # Scoring formula (same as before)
        score = (1 / cpa) * 0.5 + fulfillment * 0.3 + attendance * 0.2
        score *= 40

    except Exception as e:
        return jsonify({"error": "Invalid input", "details": str(e)}), 400

    return jsonify({
        "venue": venue,
        "score": round(score, 2),
        "recommended_time_1": "11:00 AM Monday",
        "recommended_time_2": "6:30 PM Tuesday",
        "topic": topic,
        "city": city,
        "state": state,
        "event_date": event_date,
        "event_time": event_time,
        "job_number": job_number,
        "image_allowed": image_allowed,
        "disclosure_needed": disclosure_needed
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

