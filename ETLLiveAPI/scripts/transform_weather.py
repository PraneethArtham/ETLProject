import pandas as pd
import json
from pathlib import Path

def transform_weather_data():
    BASE_DIR = Path(__file__).resolve().parents[1]
    RAW_DIR = BASE_DIR / "data" / "raw"
    STAGED_DIR = BASE_DIR / "data" / "staged"

    STAGED_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(RAW_DIR.glob("weather_*.json"))
    if not files:
        raise FileNotFoundError("No weather JSON files found in data/raw")

    latest_file = files[-1]

    with open(latest_file, "r") as f:
        data = json.load(f)

    hourly = data["hourly"]
    df = pd.DataFrame({
        "time": hourly["time"],
        "temperature_C": hourly["temperature_2m"],
        "humidity_percent": hourly["relative_humidity_2m"],
        "wind_speed_kmph": hourly["wind_speed_10m"],
    })

    df["city"] = "Hyderabad"
    df["extracted_at"] = pd.Timestamp.now()

    output_path = STAGED_DIR / "weather_cleaned.csv"
    df.to_csv(output_path, index=False)

    print(f" Transformed {len(df)} rows saved to: {output_path}")
    return df

if __name__ == "__main__":
    transform_weather_data()
