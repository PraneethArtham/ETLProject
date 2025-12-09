import os
import pandas as pd
from extract_titanic import extract_data   # from scripts/extract_titanic.py

def transform_data(raw_data: str) -> str:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)

    df = pd.read_csv(raw_data)

    # -------- 1. Handle missing values --------
    numeric_cols = ["age", "fare", "sibsp", "parch", "pclass"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())

    for col in ["embarked", "embark_town"]:
        if col in df.columns and df[col].isna().any():
            df[col] = df[col].fillna(df[col].mode()[0])

    if "deck" in df.columns:
        df["deck"] = df["deck"].fillna("Unknown")

    # -------- 2. Feature engineering --------
    if {"sibsp", "parch"}.issubset(df.columns):
        df["family_size"] = df["sibsp"] + df["parch"] + 1
    else:
        df["family_size"] = 1

    if "alone" in df.columns:
        df["is_alone"] = df["alone"].astype(int)
    else:
        df["is_alone"] = (df["family_size"] == 1).astype(int)

    if "sex" in df.columns:
        df["sex_male"] = (df["sex"] == "male").astype(int)

    if "age" in df.columns:
        df["is_child"] = (df["age"] < 18).astype(int)

    if {"fare", "family_size"}.issubset(df.columns):
        df["fare_per_person"] = df["fare"] / df["family_size"].replace(0, 1)

    # -------- 3. Optional drops --------
    cols_to_drop = ["alive", "who", "adult_male"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

    # -------- 4. Save --------
    staged_path = os.path.join(staged_dir, "titanic_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f"data transformed and saved at {staged_path}")
    return staged_path


if __name__ == "__main__":
    raw_path = extract_data()
    transform_data(raw_data=raw_path)
