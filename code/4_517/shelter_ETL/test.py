import requests
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GOOGLE_MAP_KEY_517")

query = "台北市動物之家"
url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&key={API_KEY}&language=zh-TW"
res = requests.get(url)
print(res.json())
