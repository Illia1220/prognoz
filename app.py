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
# UPLOAD CSV (STABLE VERSION)
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

        print("RAW ROWS:", len(df))

        # -----------------------------
        # CLEAN DATA
        # -----------------------------

        df = df.dropna(how="all")

        # numeric safety
        numeric_cols = ["impressions", "clicks", "spend", "conversions", "revenue"]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # date fix (IMPORTANT)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

        # remove duplicates
        df = df.drop_duplicates()

        # metrics
        df = calculate_metrics(df)

        # final cleanup
        df = df.replace({np.nan: None})

        # 🔥 CRITICAL FIX: make JSON-safe Python objects
        data = json.loads(df.to_json(orient="records"))

        print("UPLOAD ROWS:", len(data))
        print("SAMPLE:", data[0])

        # -----------------------------
        # SUPABASE INSERT
        # -----------------------------
        try:
            res = supabase.table("ads_data").insert(data).execute()

            print("SUPABASE RESPONSE:", res)

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
            print("SUPABASE INSERT ERROR:", repr(e))
            return jsonify({
                "status": "error",
                "message": repr(e)
            }), 500


    except Exception as e:
        print("UPLOAD ERROR:", repr(e))
        return jsonify({
            "status": "error",
            "message": repr(e)
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

        monthly = df.groupby(df["date"].dt.to_period("M").astype(str)).agg({
            "spend": "sum",
            "revenue": "sum",
            "clicks": "sum",
            "impressions": "sum"
        }).reset_index()

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
        print("FORECAST ERROR:", repr(e))
        return jsonify({
            "status": "error",
            "message": repr(e)
        }), 500


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)