import os
import time
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY")
)

def load_to_supabase():
    BASE_DIR = Path(__file__).resolve().parents[1]
    csv_path = BASE_DIR / "data" / "staged" / "weather_cleaned.csv"

    if not csv_path.exists():
        raise FileNotFoundError(f"Missing file: {csv_path}")

    df = pd.read_csv(csv_path)
    df = df.drop(columns=["humidity_percent"], errors="ignore")

    # Convert timestamps → strings for Supabase
    df["time"] = pd.to_datetime(df["time"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["extracted_at"] = pd.to_datetime(df["extracted_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    print("Columns being inserted:", df.columns.tolist())

    batch_size = 20

    for i in range(0, len(df), batch_size):
        batch = (
            df.iloc[i:i + batch_size]
            .where(pd.notnull(df), None)
            .to_dict("records")
        )

        supabase.table("weather_data").insert(batch).execute()

        print(f"Inserted rows {i + 1}–{min(i + batch_size, len(df))}")
        time.sleep(0.5)

    print(" Finished loading weather data into Supabase")

if __name__ == "__main__":
    load_to_supabase()
