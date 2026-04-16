from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from db import supabase
import io
import numpy as np

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

    df = df.replace([np.inf, -np.inf], 0)

    return df


# -----------------------------
# UPLOAD CSV
# -----------------------------
@app.route("/upload", methods=["POST"])
def upload_csv():
    try:
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files["file"]

        if file.filename == "":
            return jsonify({"error": "Empty file"}), 400

        content = file.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(content))

        df = calculate_metrics(df)

        data = df.to_dict(orient="records")

        supabase.table("ads_data").insert(data).execute()

        return jsonify({
            "status": "success",
            "rows_inserted": len(df)
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# -----------------------------
# FORECAST (FIXED)
# -----------------------------
@app.route("/forecast", methods=["GET"])
def forecast():
    try:
        response = supabase.table("ads_data").select("*").execute()
        data = response.data or []

        if len(data) == 0:
            return jsonify({
                "next_month_roi": 0,
                "roi_trend": 0,
                "recommended_spend": 0,
                "monthly": []
            })

        df = pd.DataFrame(data)

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        df = calculate_metrics(df)

        # -------------------------
        # MONTHLY AGGREGATION
        # -------------------------
        monthly = df.groupby(df["date"].dt.to_period("M").astype(str)).agg({
            "spend": "sum",
            "revenue": "sum",
            "clicks": "sum",
            "impressions": "sum"
        }).reset_index()

        monthly.rename(columns={"date": "month"}, inplace=True)

        monthly["roi"] = (
            monthly["revenue"] / monthly["spend"].replace(0, 1)
        ).fillna(0)

        monthly = monthly.replace([np.inf, -np.inf], 0)

        # -------------------------
        # TREND
        # -------------------------
        if len(monthly) > 1:
            roi_trend = (
                monthly["roi"].iloc[-1] - monthly["roi"].iloc[0]
            ) / len(monthly)
        else:
            roi_trend = 0

        avg_roi = monthly["roi"].mean()
        next_month_roi = avg_roi + roi_trend

        # -------------------------
        # RECOMMENDED SPEND
        # -------------------------
        avg_spend = monthly["spend"].mean()

        if next_month_roi <= 0:
            recommended_spend = avg_spend
        else:
            recommended_spend = avg_spend * (avg_roi / next_month_roi)

        return jsonify({
            "monthly": monthly.to_dict(orient="records"),
            "next_month_roi": float(next_month_roi),
            "roi_trend": float(roi_trend),
            "recommended_spend": float(recommended_spend)
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)