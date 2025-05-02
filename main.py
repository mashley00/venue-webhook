from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/", methods=["GET"])
def health_check():
    return "OK", 200


@app.route("/analyze_venue", methods=["POST"])
def analyze_venue():
    data = request.json

    try:
        cpa = float(data["CPA"])
        fulfillment = float(data["Fulfillment_Percent"])
        attendance = float(data["Attendance_Rate"])

        score = (1 / cpa) * 0.5 + fulfillment * 0.3 + attendance * 0.2
        score *= 40
    except Exception as e:
        return jsonify({"error": "Invalid input format", "details": str(e)}), 400

    return jsonify({
        "venue": data["Venue"],
        "score": round(score, 2),
        "recommended_time_1": "11:00 AM Monday",
        "recommended_time_2": "6:30 PM Tuesday"
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
