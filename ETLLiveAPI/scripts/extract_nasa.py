from pathlib import Path
import json
from datetime import datetime
import requests
DATA_DIR=Path(__file__).resolve().parents[1]/"data"/"raw"
DATA_DIR.mkdir(parents=True,exist_ok=True)
def extract_nasa():
    url="https://api.nasa.gov/planetary/apod?api_key=dMo7SdEBsOpgqtRxMx0vKgaIl76NqR02ebyL1fhu"

    resp=requests.get(url)
    resp.raise_for_status()
    data=resp.json()

    filename=DATA_DIR/f"nasa_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filename.write_text(json.dumps(data,indent=2))

    print(f"Extracted data saved at:{filename}")
    return data
if __name__=="__main__":
    extract_nasa()