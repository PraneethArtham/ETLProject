from supabase import create_client
from dotenv import load_dotenv
import os
import pandas as pd
from pathlib import Path

# Load environment variables
load_dotenv()

# Create Supabase client
supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_KEY"),
)

def load_nasa():
    BASE_DIR = Path(__file__).resolve().parents[1]
    csv_path = BASE_DIR / "data" / "staged" / "nasa_cleaned.csv"

    if not csv_path.exists():
        raise FileNotFoundError(f"Missing file: {csv_path}")

    df = pd.read_csv(csv_path)

    # Convert timestamps to ISO format
    if "inserted_at" in df.columns:
        df["inserted_at"] = pd.to_datetime(df["inserted_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    elif "extracted_at" in df.columns:
        df["extracted_at"] = pd.to_datetime(df["extracted_at"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
        df = df.rename(columns={"extracted_at": "inserted_at"})

    print("Columns being inserted:", df.columns.tolist())

    # Insert each row into Supabase table
    for _, row in df.iterrows():
        record = row.to_dict()
        response = supabase.table("nasa_apod").insert(record).execute()

if __name__ == "__main__":
    load_nasa()
