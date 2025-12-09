import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("Missing Supabase URL or KEY in .env")
    return create_client(url, key)

def load_data_to_supabase(staged_path: str, table_name: str = "iris_data"):
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))
    print(f"Looking for the data file at: {staged_path}")

    if not os.path.exists(staged_path):
        print(f"Error: file not found at {staged_path}")
        return

    supabase = get_supabase_client()
    df = pd.read_csv(staged_path)

    # Remove 'id' column if exists
    if 'id' in df.columns:
        df = df.drop(columns=['id'])

    total_rows = len(df)
    batch_size = 100
    print(f"Loading {total_rows} rows into '{table_name}'...")

    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size].copy()
        batch = batch.where(pd.notnull(batch), None)
        records = batch.to_dict("records")
        try:
            supabase.table(table_name).insert(records).execute()
            end = min(i + batch_size, total_rows)
            print(f"Inserted rows {i + 1} --- {end} of {total_rows}")
        except Exception as e:
            print(f"Error in batch {i // batch_size + 1}: {str(e)}")
            continue

    print(f"Finished loading data into '{table_name}'.")

if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "iris_transformed.csv")
    load_data_to_supabase(staged_csv_path)
