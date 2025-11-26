# E_shelter.py – 正確動物收容所資料來源（官方 OpenData）

import requests
import pandas as pd
import os
import time

RAW_DIR = os.path.join(os.getcwd(), "data", "raw", "shelter")
os.makedirs(RAW_DIR, exist_ok=True)
RAW_PATH = os.path.join(RAW_DIR, "shelter_raw.csv")

# 農業部動物收容所資料（官方 OpenData API）
MOA_API_URL = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=2thVboChxuKs"



def fetch_raw_data(max_retries=3, timeout=30, use_cache=True):
    print("🐾 [E] Extract - 抓取農業部動物收容所資料中...")

    # 是否使用快取
    if use_cache and os.path.exists(RAW_PATH):
        try:
            df = pd.read_csv(RAW_PATH)
            print(f"📂 偵測到快取檔案：{RAW_PATH}")
            print(f"✅ 已從快取載入 {len(df)} 筆資料")
            return df
        except Exception:
            print("⚠️ 快取損毀，重新下載")

    # API 下載
    for attempt in range(1, max_retries + 1):
        try:
            print(f"🔄 第 {attempt} 次連線中...")
            resp = requests.get(MOA_API_URL, timeout=timeout)
            resp.raise_for_status()

            data = resp.json()
            df = pd.DataFrame(data)

            df.rename(columns={
                "shelterName": "name",
                "shelterAddress": "address",
                "shelterTel": "phone",
                "cityName": "city",
                "placeName": "district"
            }, inplace=True)

            df.to_csv(RAW_PATH, index=False, encoding="utf-8-sig")
            print(f"📦 已成功抓取 {len(df)} 筆資料寫入 {RAW_PATH}")
            return df

        except Exception as e:
            print(f"⚠️ 抓取失敗：{e}")
            time.sleep(5)

    print("❌ 無法取得農業部收容所資料")
    return pd.DataFrame()
