from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
from db import supabase
import io
import numpy as np
import json

app = Flask(__name__)
CORS(app)

# -----------------------------
# METRICS
# -----------------------------
def calculate_metrics(df):
    df = df.fillna(0)

    df["ctr"] = df["clicks"] / df["impressions"].replace(0, np.nan)
    df["cpm"] = df["spend"] / df["impressions"].replace(0, np.nan) * 1000
    df["cpa"] = df["spend"] / df["conversions"].replace(0, np.nan)
    df["roi"] = df["revenue"] / df["spend"].replace(0, np.nan)

    df = df.replace([np.inf, -np.inf], 0)
    df = df.fillna(0)

    return df


# -----------------------------
# CLEAR (FIXED)
# -----------------------------
@app.route("/clear", methods=["POST"])
def clear():
    try:
        supabase.table("ads_data").delete().gte("date", "1900-01-01").execute()
        return jsonify({"status": "cleared"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# -----------------------------
# UPLOAD CSV (REPLACE MODE)
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

        allowed_cols = [
            "date","campaign","geo",
            "impressions","clicks","spend",
            "conversions","revenue"
        ]

        df = df[allowed_cols]

        print("RAW ROWS:", len(df))

        df = df.dropna(how="all")

        numeric_cols = ["impressions", "clicks", "spend", "conversions", "revenue"]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

        df = df.drop_duplicates()

        df = calculate_metrics(df)

        df = df.replace({np.nan: None})

        data = json.loads(df.to_json(orient="records"))

        # 🔥 IMPORTANT FIX: REPLACE MODE (no duplicates ever)
        supabase.table("ads_data").delete().gte("date", "1900-01-01").execute()

        res = supabase.table("ads_data").insert(data).execute()

        if hasattr(res, "error") and res.error:
            return jsonify({
                "status": "error",
                "message": str(res.error)
            }), 500

        return jsonify({
            "status": "success",
            "rows_inserted": len(data)
        })

    except Exception as e:
        print("UPLOAD ERROR:", repr(e))
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# -----------------------------
# FORECAST
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
                "monthly": [],
                "forecast_point": None
            })

        df = pd.DataFrame(data)

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        df = calculate_metrics(df)

        monthly = df.groupby(df["date"].dt.to_period("M")).agg({
            "spend": "sum",
            "revenue": "sum",
            "clicks": "sum",
            "impressions": "sum"
        }).reset_index()

        monthly["date"] = monthly["date"].astype(str)

        monthly["roi"] = (
            monthly["revenue"] /
            monthly["spend"].replace(0, np.nan)
        ).fillna(0)

        monthly = monthly.replace([np.inf, -np.inf], 0)

        if len(monthly) > 1:
            roi_trend = (
                monthly["roi"].iloc[-1] -
                monthly["roi"].iloc[0]
            ) / len(monthly)
        else:
            roi_trend = 0

        avg_roi = monthly["roi"].mean()
        next_month_roi = avg_roi + roi_trend

        last_spend = monthly["spend"].iloc[-1]

        # базовый коэффициент ROI
        roi_factor = 1

        if next_month_roi > avg_roi:
            roi_factor = 1.15
        elif next_month_roi < avg_roi:
            roi_factor = 0.90

        # CTR фактор
        ctr_factor = 1

        if "clicks" in monthly.columns and "impressions" in monthly.columns:
            monthly["ctr"] = monthly["clicks"] / monthly["impressions"].replace(0, np.nan)
            last_ctr = monthly["ctr"].iloc[-1]
            avg_ctr = monthly["ctr"].mean()

            if last_ctr > avg_ctr:
                ctr_factor = 1.05
            else:
                ctr_factor = 0.95

        # итог
        recommended_spend = last_spend * roi_factor * ctr_factor

        # safety limits
        max_up = last_spend * 1.20
        max_down = last_spend * 0.70

        recommended_spend = min(recommended_spend, max_up)
        recommended_spend = max(recommended_spend, max_down)




        last_month = pd.Period(monthly["date"].iloc[-1], freq="M")
        next_month = str(last_month + 1)

        forecast_point = {
            "date": next_month,
            "roi": float(next_month_roi)
        }

        return jsonify({
            "monthly": monthly.to_dict(orient="records"),
            "next_month_roi": float(next_month_roi),
            "roi_trend": float(roi_trend),
            "recommended_spend": float(recommended_spend),
            "forecast_point": forecast_point
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