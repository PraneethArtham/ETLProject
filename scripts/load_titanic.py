import os
import pandas as pd
from supabase import create_client
import dotenv

# -----------------------------
# Load environment variables
# -----------------------------
dotenv.load_dotenv()  # Make sure .env is in project root

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # Use service role key here

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY in .env")

# -----------------------------
# Client (service role used for all operations)
# -----------------------------
client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# Load Data into Supabase
# -----------------------------
def load_to_supabase(staged_path: str, table_name: str = "titanic_data"):
    # Make path absolute
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))

    if not os.path.exists(staged_path):
        raise FileNotFoundError(f"File not found at {staged_path}")

    # Read CSV
    df = pd.read_csv(staged_path)
    total_rows = len(df)
    batch_size = 50

    print(f"Loading {total_rows} rows into Supabase table '{table_name}' ...")

    # Insert in batches
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size].copy()
        batch = batch.where(pd.notnull(batch), None)  # NaN -> None
        records = batch.to_dict("records")

        try:
            client.table(table_name).insert(records).execute()
            print(f"Inserted rows {i + 1}-{min(i + batch_size, total_rows)}")
        except Exception as e:
            print(f"Batch {i // batch_size + 1} failed:", e)

    print("Finished loading Titanic data into Supabase!")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    staged_csv_path = os.path.join("..", "data", "staged", "titanic_transformed.csv")

    print("⚠️ Make sure the table 'titanic_data' exists in Supabase before running this script!")
    load_to_supabase(staged_csv_path)
