from flask import Flask, request, jsonify
import pandas as pd
from db import supabase
import io

app = Flask(__name__)


def calculate_metrics(df):
    # защита от нулей и ошибок
    df = df.fillna(0)

    df["ctr"] = df["clicks"] / df["impressions"].replace(0, 1)
    df["cpm"] = df["spend"] / df["impressions"].replace(0, 1) * 1000
    df["cpa"] = df["spend"] / df["conversions"].replace(0, 1)
    df["roi"] = df["revenue"] / df["spend"].replace(0, 1)

    return df


@app.route("/")
def home():
    return "CSV ETL Service is running 🚀"


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

        response = supabase.table("ads_data").insert(data).execute()

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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)