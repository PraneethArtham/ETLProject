from supabase import create_client
import pandas as pd, os
from dotenv import load_dotenv
load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
data = supabase.table("iris_data").select("*").execute()
df = pd.DataFrame(data.data)
df.head()
df.info()
df.isnull().sum()
df.describe()
df.columns