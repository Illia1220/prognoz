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

    df["ctr"] = df["clicks"] / df["impressions"].replace(0, np.nan)
    df["cpm"] = df["spend"] / df["impressions"].replace(0, np.nan) * 1000
    df["cpa"] = df["spend"] / df["conversions"].replace(0, np.nan)
    df["roi"] = df["revenue"] / df["spend"].replace(0, np.nan)

    df = df.replace([np.inf, -np.inf], 0)
    df = df.fillna(0)

    return df


# -----------------------------
# UPLOAD CSV (FIXED)
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

        # -----------------------------
        # CLEANING (IMPORTANT FIX)
        # -----------------------------

        # remove empty rows
        df = df.dropna(how="all")

        # convert NaN → None (Supabase requirement)
        df = df.replace({np.nan: None})

        # numeric safety
        numeric_cols = ["impressions", "clicks", "spend", "conversions", "revenue"]

        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # date fix
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
        df = df.dropna(subset=["date"])

        # remove duplicates inside file
        df = df.drop_duplicates()

        # calculate metrics
        df = calculate_metrics(df)

        # final cleanup (VERY IMPORTANT)
        df = df.replace({np.nan: None})

        data = df.to_dict(orient="records")

        print(f"UPLOAD ROWS: {len(data)}")

        # -----------------------------
        # INSERT WITH DEBUG
        # -----------------------------
        res = supabase.table("ads_data").insert(data).execute()

        # debug Supabase response
        if hasattr(res, "error") and res.error:
            print("SUPABASE ERROR:", res.error)
            return jsonify({
                "status": "error",
                "message": str(res.error)
            }), 500

        return jsonify({
            "status": "success",
            "rows_inserted": len(data)
        })

    except Exception as e:
        print("UPLOAD FAILED:", str(e))
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
                "monthly": []
            })

        df = pd.DataFrame(data)

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        df = calculate_metrics(df)

        monthly = df.groupby(df["date"].dt.to_period("M").astype(str)).agg({
            "spend": "sum",
            "revenue": "sum",
            "clicks": "sum",
            "impressions": "sum"
        }).reset_index()

        monthly.rename(columns={"date": "month"}, inplace=True)

        monthly["roi"] = (
            monthly["revenue"] / monthly["spend"].replace(0, np.nan)
        ).fillna(0)

        monthly = monthly.replace([np.inf, -np.inf], 0)

        if len(monthly) > 1:
            roi_trend = (
                monthly["roi"].iloc[-1] - monthly["roi"].iloc[0]
            ) / len(monthly)
        else:
            roi_trend = 0

        avg_roi = monthly["roi"].mean()
        next_month_roi = avg_roi + roi_trend

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
        print("FORECAST ERROR:", str(e))
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)