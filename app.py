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
    try:
        # берем данные из Supabase
        response = supabase.table("ads_data").select("*").execute()

        data = response.data

        if not data:
            return jsonify({"error": "No data"}), 400

        df = pd.DataFrame(data)
        df = calculate_metrics(df)

        # -------------------------
        # GLOBAL FORECAST
        # -------------------------
        avg_roi = df["roi"].mean()
        avg_spend = df["spend"].mean()
        avg_revenue = df["revenue"].mean()

        trend_roi = (df["roi"].iloc[-1] - df["roi"].iloc[0]) / max(len(df), 1)

        forecast_7d_roi = avg_roi + trend_roi * 7
        forecast_7d_revenue = avg_revenue * 7
        forecast_7d_spend = avg_spend * 7

        # -------------------------
        # BY CAMPAIGN FORECAST
        # -------------------------
        campaign_forecast = (
            df.groupby("campaign")["roi"]
            .mean()
            .reset_index()
            .to_dict(orient="records")
        )

        return jsonify({
            "avg_roi": float(avg_roi),
            "avg_spend": float(avg_spend),
            "avg_revenue": float(avg_revenue),

            "trend_roi": float(trend_roi),

            "forecast_7d": {
                "roi": float(forecast_7d_roi),
                "spend": float(forecast_7d_spend),
                "revenue": float(forecast_7d_revenue),
            },

            "campaign_forecast": campaign_forecast
        })

    except Exception as e:
        print("❌ Forecast error:", str(e))

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)