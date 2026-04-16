from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from db import supabase
import io

app = Flask(__name__)
CORS(app)


# -----------------------------
# METRICS
# -----------------------------
def calculate_metrics(df):
    df = df.fillna(0)

    df["ctr"] = df["clicks"] / df["impressions"].replace(0, 1)
    df["cpm"] = df["spend"] / df["impressions"].replace(0, 1) * 1000
    df["cpa"] = df["spend"] / df["conversions"].replace(0, 1)
    df["roi"] = df["revenue"] / df["spend"].replace(0, 1)

    return df


# -----------------------------
# HOME
# -----------------------------
@app.route("/")
def home():
    return "CSV ETL Service is running 🚀"


# -----------------------------
# UPLOAD CSV
# -----------------------------
@app.route("/upload", methods=["POST"])
def upload_csv():
    try:
        print("📥 Upload request received")

        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files["file"]

        if file.filename == "":
            return jsonify({"error": "Empty file"}), 400

        content = file.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(content))

        print(f"📊 Rows received: {len(df)}")

        df = calculate_metrics(df)

        data = df.to_dict(orient="records")

        supabase.table("ads_data").insert(data).execute()

        print("✅ Data inserted to Supabase")

        return jsonify({
            "status": "success",
            "rows_inserted": len(df)
        })

    except Exception as e:
        print("❌ Error:", str(e))

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# -----------------------------
# FORECAST ENDPOINT
# -----------------------------
@app.route("/forecast", methods=["GET"])
def forecast():
    response = supabase.table("ads_data").select("*").execute()
    data = response.data

    if not data:
        return {"error": "No data"}

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    df = calculate_metrics(df)

    # -------------------------
    # MONTHLY AGGREGATION
    # -------------------------
    monthly = df.groupby(df["date"].dt.to_period("M")).agg({
        "spend": "sum",
        "revenue": "sum",
        "clicks": "sum",
        "impressions": "sum"
    }).reset_index()

    monthly["roi"] = monthly["revenue"] / monthly["spend"].replace(0, 1)

    # -------------------------
    # TREND
    # -------------------------
    roi_trend = (
        monthly["roi"].iloc[-1] - monthly["roi"].iloc[0]
    ) / max(len(monthly), 1)

    next_month_roi = monthly["roi"].mean() + roi_trend

    # -------------------------
    # RECOMMENDED SPEND
    # -------------------------
    avg_roi = monthly["roi"].mean()

    recommended_spend = (
        monthly["spend"].mean() * (avg_roi / max(next_month_roi, 0.1))
    )

    return {
        "monthly": monthly.to_dict(orient="records"),
        "next_month_roi": float(next_month_roi),
        "roi_trend": float(roi_trend),
        "recommended_spend": float(recommended_spend)
    }


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)