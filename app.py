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

        monthly["date"] = monthly["date"].dt.strftime("%Y-%m")
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

        roi_factor = 1
        if next_month_roi > avg_roi:
            roi_factor = 1.15
        elif next_month_roi < avg_roi:
            roi_factor = 0.90

        ctr_factor = 1
        monthly["ctr"] = monthly["clicks"] / monthly["impressions"].replace(0, np.nan)

        last_ctr = monthly["ctr"].iloc[-1]
        avg_ctr = monthly["ctr"].mean()

        if last_ctr > avg_ctr:
            ctr_factor = 1.05
        else:
            ctr_factor = 0.95

        recommended_spend = last_spend * roi_factor * ctr_factor

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


from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter


@app.route("/export", methods=["GET"])
def export_excel():
    try:
        response = supabase.table("ads_data").select("*").execute()
        data = response.data or []

        if len(data) == 0:
            return jsonify({"error": "No data"}), 400

        df = pd.DataFrame(data)

        # 🔥 FIX DATE
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        df = calculate_metrics(df)

        # -----------------------------
        # FORECAST
        # -----------------------------
        monthly = df.groupby(df["date"].dt.to_period("M")).agg({
            "spend": "sum",
            "revenue": "sum"
        }).reset_index()

        monthly["roi"] = (
            monthly["revenue"] /
            monthly["spend"].replace(0, np.nan)
        ).fillna(0)

        # -----------------------------
        campaign_stats = df.groupby("campaign").agg({
            "spend": "sum",
            "revenue": "sum"
        }).reset_index()

        campaign_stats["roi"] = (
            campaign_stats["revenue"] /
            campaign_stats["spend"].replace(0, np.nan)
        ).fillna(0)

        global_avg_roi = campaign_stats["roi"].mean()

        campaign_stats["predicted_roi"] = campaign_stats["roi"]
        campaign_stats["recommended_spend"] = (
            campaign_stats["spend"] *
            (campaign_stats["predicted_roi"] / (global_avg_roi if global_avg_roi != 0 else 1))
        )

        # -----------------------------
        # EXCEL
        # -----------------------------
        wb = Workbook()

        header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True, size=12)

        def style_sheet(ws):
            # freeze header
            ws.freeze_panes = "A2"

            # header style
            for cell in ws[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal="center")

            # auto width
            for col in ws.columns:
                max_length = 0
                col_letter = get_column_letter(col[0].column)

                for cell in col:
                    try:
                        value = str(cell.value)
                        if len(value) > max_length:
                            max_length = len(value)
                    except:
                        pass

                ws.column_dimensions[col_letter].width = max_length + 3

        # ---------------- RAW DATA ----------------
        ws1 = wb.active
        ws1.title = "Raw Data"

        export_df = df.merge(
            campaign_stats[["campaign", "predicted_roi", "recommended_spend"]],
            on="campaign",
            how="left"
        )

        # 🔥 FIX DATE FORMAT (IMPORTANT)
        export_df["date"] = pd.to_datetime(export_df["date"]).dt.strftime("%d.%m.%Y %H:%M:%S")

        ws1.append(list(export_df.columns))

        for row in export_df.itertuples(index=False):
            ws1.append(list(row))

        style_sheet(ws1)

        # ---------------- SUMMARY ----------------
        ws2 = wb.create_sheet("Campaign Forecast")
        ws2.append(["Campaign", "ROI", "Predicted ROI", "Recommended Spend"])

        for row in campaign_stats.itertuples(index=False):
            ws2.append([
                row.campaign,
                float(row.roi),
                float(row.predicted_roi),
                float(row.recommended_spend)
            ])

        style_sheet(ws2)

        # ---------------- SAVE ----------------
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