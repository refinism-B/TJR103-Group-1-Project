import os
from dotenv import load_dotenv

# === 自動尋找專案根目錄（TJR103GROUP1） ===
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
env_path = os.path.join(BASE_DIR, ".env")

# === 載入環境變數 ===
load_dotenv(dotenv_path=env_path)

# === Google Maps API Key ===
API_KEY = os.getenv("GOOGLE_MAP_KEY_517")
API_LINK = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=2thVboChxuKs"

# === 資料輸出設定 ===
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(DATA_DIR, "taiwan_pet_shelters_with_google.csv")

# === MySQL 設定 ===
MYSQL = {
    "username": os.getenv("MYSQL_USERNAME"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "ip": os.getenv("MYSQL_IP"),
    "port": int(os.getenv("MYSQL_PORTT", 3306)),
    "db_name": os.getenv("MYSQL_DB_NAME")
}
