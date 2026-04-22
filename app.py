from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
from db import supabase
import io
import numpy as np
import json
import tempfile

from openpyxl import Workbook
from openpyxl.chart import LineChart, Reference

app = Flask(__name__)
CORS(app)

# -----------------------------
# SAFE DIVISION
# -----------------------------
def safe_div(a, b):
    return np.where((b == 0) | (pd.isna(b)), np.nan, a / b)


# -----------------------------
# METRICS
# -----------------------------
def calculate_metrics(df):
    df = df.copy()

    # приводим числа
    for col in ["impressions", "clicks", "spend", "conversions", "revenue"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ctr"] = safe_div(df["clicks"], df["impressions"])
    df["cpm"] = safe_div(df["spend"], df["impressions"]) * 1000
    df["cpa"] = safe_div(df["spend"], df["conversions"])
    df["roi"] = safe_div(df["revenue"], df["spend"])

    # заменяем только inf, но НЕ убиваем nan заранее
    df = df.replace([np.inf, -np.inf], np.nan)

    return df


# -----------------------------
# CLEAR
# -----------------------------
@app.route("/clear", methods=["POST"])
def clear():
    try:
        supabase.table("ads_data").delete().gte("date", "1900-01-01").execute()
        return jsonify({"status": "cleared"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


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

        allowed_cols = [
            "date", "campaign", "geo",
            "impressions", "clicks", "spend",
            "conversions", "revenue"
        ]

        df = df[allowed_cols]
        df = df.dropna(how="all")

        numeric_cols = [
            "impressions",
            "clicks",
            "spend",
            "conversions",
            "revenue"
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

        df = df.drop_duplicates()
        df = calculate_metrics(df)
        df = df.replace({np.nan: None})

        data = json.loads(df.to_json(orient="records"))

        # replace mode
        supabase.table("ads_data").delete().gte("date", "1900-01-01").execute()
        supabase.table("ads_data").insert(data).execute()

        return jsonify({
            "status": "success",
            "rows_inserted": len(data)
        })

    except Exception as e:
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
                "monthly": [],
                "metrics": {},
                "recommended_spend": 0,
                "forecast_point": None
            })

        metrics = request.args.get("metrics", "roi").split(",")

        df = pd.DataFrame(data)

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        df = calculate_metrics(df)

        # -----------------------------
        # MONTHLY AGGREGATION
        # -----------------------------
        monthly = df.groupby(df["date"].dt.to_period("M")).agg({
            "spend": "sum",
            "revenue": "sum",
            "clicks": "sum",
            "impressions": "sum",
            "conversions": "sum"
        }).reset_index()

        monthly["date"] = monthly["date"].dt.strftime("%Y-%m")

        # -----------------------------
        # SAFE DIVISION (CRITICAL FIX)
        # -----------------------------
        def safe_div(a, b):
            return np.where((b == 0) | (pd.isna(b)), np.nan, a / b)

        # -----------------------------
        # METRIC SERIES (FIXED)
        # -----------------------------
        def get_metric_series(metric):
            if metric == "roi":
                return safe_div(monthly["revenue"], monthly["spend"])

            if metric == "cpa":
                return safe_div(monthly["spend"], monthly["conversions"])

            if metric == "ctr":
                return safe_div(monthly["clicks"], monthly["impressions"])

            return None

        results = {}

        # -----------------------------
        # FORECAST ENGINE
        # -----------------------------
        for metric in metrics:
            series = get_metric_series(metric)

            if series is None:
                continue

            monthly[metric] = series

            avg = np.nanmean(series)

            if len(series) > 1:
                trend = (series[-1] - series[0]) / len(series)
            else:
                trend = 0

            next_value = avg + trend

            results[metric] = {
                "history": [None if np.isnan(x) else float(x) for x in series],
                "avg": float(avg) if not np.isnan(avg) else 0,
                "trend": float(trend),
                "next_value": float(next_value) if not np.isnan(next_value) else 0
            }

        # -----------------------------
        # RECOMMENDED SPEND LOGIC
        # -----------------------------
        last_spend = monthly["spend"].iloc[-1]
        factor = 1

        if "roi" in results:
            factor += 0.15 if results["roi"]["next_value"] > results["roi"]["avg"] else -0.10

        if "cpa" in results:
            factor += 0.10 if results["cpa"]["next_value"] < results["cpa"]["avg"] else -0.10

        if "ctr" in results:
            factor += 0.05 if results["ctr"]["next_value"] > results["ctr"]["avg"] else -0.05

        recommended_spend = last_spend * factor
        recommended_spend = min(recommended_spend, last_spend * 1.20)
        recommended_spend = max(recommended_spend, last_spend * 0.70)

        # -----------------------------
        # FORECAST POINT
        # -----------------------------
        last_month = pd.Period(monthly["date"].iloc[-1], freq="M")
        next_month = str(last_month + 1)

        forecast_point = {
            "date": next_month,
            **{m: results[m]["next_value"] for m in results}
        }

        return jsonify({
            "monthly": monthly.replace([np.inf, -np.inf], np.nan).fillna(0).to_dict(orient="records"),
            "metrics": results,
            "recommended_spend": float(recommended_spend),
            "forecast_point": forecast_point
        })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# -----------------------------
@app.route("/export", methods=["GET"])
def export_excel():
    try:
        response = supabase.table("ads_data").select("*").execute()
        data = response.data or []

        if len(data) == 0:
            return jsonify({"error": "No data"}), 400

        df = pd.DataFrame(data)

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        df = calculate_metrics(df)

        # -----------------------------
        # MONTHLY
        # -----------------------------
        def safe_div(a, b):
            return np.where((b == 0) | (pd.isna(b)), np.nan, a / b)

        monthly = df.groupby(df["date"].dt.to_period("M")).agg({
            "spend": "sum",
            "revenue": "sum"
        }).reset_index()

        monthly["roi"] = safe_div(monthly["revenue"], monthly["spend"])

        # -----------------------------
        campaign_stats = df.groupby("campaign").agg({
            "spend": "sum",
            "revenue": "sum"
        }).reset_index()

        campaign_stats["roi"] = safe_div(
            campaign_stats["revenue"],
            campaign_stats["spend"]
        )

        global_avg_roi = np.nanmean(campaign_stats["roi"])

        campaign_stats["predicted_roi"] = campaign_stats["roi"]
        campaign_stats["recommended_spend"] = campaign_stats["spend"] * (
            campaign_stats["predicted_roi"] / (global_avg_roi if global_avg_roi else 1)
        )

        # -----------------------------
        # EXCEL
        # -----------------------------
        wb = Workbook()

        header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True, size=12)

        def style_sheet(ws):
            ws.freeze_panes = "A2"

            for cell in ws[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal="center")

            for col in ws.columns:
                max_length = 0
                col_letter = get_column_letter(col[0].column)

                for cell in col:
                    try:
                        value = str(cell.value)
                        max_length = max(max_length, len(value))
                    except:
                        pass

                ws.column_dimensions[col_letter].width = max_length + 3

        # RAW DATA
        ws1 = wb.active
        ws1.title = "Raw Data"

        export_df = df.merge(
            campaign_stats[["campaign", "predicted_roi", "recommended_spend"]],
            on="campaign",
            how="left"
        )

        export_df["date"] = pd.to_datetime(export_df["date"]).dt.strftime("%d.%m.%Y %H:%M:%S")

        ws1.append(list(export_df.columns))

        for row in export_df.itertuples(index=False):
            ws1.append(list(row))

        style_sheet(ws1)

        # SUMMARY
        ws2 = wb.create_sheet("Campaign Forecast")
        ws2.append(["Campaign", "ROI", "Predicted ROI", "Recommended Spend"])

        for row in campaign_stats.itertuples(index=False):
            ws2.append([
                row.campaign,
                float(row.roi) if not np.isnan(row.roi) else 0,
                float(row.predicted_roi) if not np.isnan(row.predicted_roi) else 0,
                float(row.recommended_spend)
            ])

        style_sheet(ws2)

        # SAVE
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
        wb.save(tmp.name)

        return send_file(
            tmp.name,
            as_attachment=True,
            download_name="analytics_report.xlsx"
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)