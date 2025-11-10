import os
import requests
import pandas as pd
import urllib3
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === 初始化 ===
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))
API_LINK = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=2thVboChxuKs"

# === 路徑設定 ===
RAW_DIR = os.path.join(os.getcwd(), "data", "raw", "shelter")
os.makedirs(RAW_DIR, exist_ok=True)
RAW_PATH = os.path.join(RAW_DIR, "shelter_raw.csv")

def fetch_raw_data():
    print("🐾 正在從農業部 API 抓取資料...")
    res = requests.get(API_LINK, verify=False, timeout=15)
    res.raise_for_status()
    data = res.json()

    df = pd.DataFrame(data)
    print(f"📋 共取得 {len(df)} 筆全台收容所資料")

    df = df[["ShelterName", "CityName", "Address", "Phone"]].copy()
    df.rename(columns={
        "ShelterName": "name",
        "CityName": "city",
        "Address": "address",
        "Phone": "phone"
    }, inplace=True)

    df.to_csv(RAW_PATH, index=False, encoding="utf-8-sig")
    print(f"📦 已儲存原始資料至：{RAW_PATH}")
    return df
