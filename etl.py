import pandas as pd
from db import supabase

def calculate_metrics(df):
    df["ctr"] = df["clicks"] / df["impressions"]
    df["cpm"] = df["spend"] / df["impressions"] * 1000
    df["cpa"] = df["spend"] / df["conversions"]
    df["roi"] = df["revenue"] / df["spend"]
    return df


def load_to_supabase(df):
    data = df.to_dict(orient="records")
    supabase.table("ads_data").insert(data).execute()


def run_etl():
    # пока тестовые данные
    df = pd.read_csv("/Users/betpublic/Desktop/prognoz/backend/data.csv")
    df = calculate_metrics(df)
    load_to_supabase(df)


if __name__ == "__main__":
    run_etl()