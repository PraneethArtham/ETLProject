import os 
import pandas as pd
import json
from pathlib import Path


def transform_nasa():
    BASE_DIR = Path(__file__).resolve().parents[1]
    RAW_DIR = BASE_DIR / "data" / "raw"
    STAGED_DIR = BASE_DIR / "data" / "staged"
    STAGED_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(RAW_DIR.glob("nasa_*.json"))
    if not files:
        raise FileNotFoundError("No nasa JSON files found in data/raw")

    latest_file = files[-1]

    with open(latest_file, "r") as f:
        data = json.load(f)
    df=pd.DataFrame({
        "date": [data.get("date")],
        "title": [data.get("title")],
        "explanation": [data.get("explanation")],
        "media_type": [data.get("media_type")],
        "image_url": [data.get("url")],
        "copyright": [data.get("copyright")],
        "inserted_at": [pd.Timestamp.now()]
    })
    output_path=STAGED_DIR/"nasa_cleaned.csv"
    df.to_csv(output_path,index=False)

    print(f"transformed data saved at:{output_path}")

if __name__=="__main__":
    transform_nasa()