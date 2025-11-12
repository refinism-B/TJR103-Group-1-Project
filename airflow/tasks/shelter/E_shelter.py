# E_shelter.py
import requests
import pandas as pd
import os
import time

# === 檔案設定 ===
RAW_DIR = os.path.join(os.getcwd(), "data", "raw", "shelter")
os.makedirs(RAW_DIR, exist_ok=True)
RAW_PATH = os.path.join(RAW_DIR, "shelter_raw.csv")

# === 農業部 OpenData API ===
MOA_API_URL = "https://data.moa.gov.tw/Service/OpenData/ODwsv/ODwsvTravelFood.aspx?FName=animal_adopt"


def fetch_raw_data(max_retries=3, timeout=60, use_cache=True):
    """
    抓取農業部動物收容所資料（含重試與快取機制）
    """
    print("🐾 [E] Extract - 抓取農業部資料中...")

    # === 快取檢查 ===
    if use_cache and os.path.exists(RAW_PATH):
        print(f"📂 偵測到快取檔案：{RAW_PATH}")
        try:
            df_cached = pd.read_csv(RAW_PATH)
            print(f"✅ 已從快取載入 {len(df_cached)} 筆資料")
            return df_cached
        except Exception as e:
            print(f"⚠️ 快取載入失敗：{e}，改為重新抓取資料。")

    # === 抓取 API 資料 ===
    for attempt in range(1, max_retries + 1):
        try:
            print(f"🔄 嘗試第 {attempt} 次連線中 (timeout={timeout}s)...")
            resp = requests.get(MOA_API_URL, timeout=timeout)
            resp.raise_for_status()

            data = resp.json()
            if not data:
                raise ValueError("API 回傳空資料")

            df = pd.DataFrame(data)
            df = df.rename(columns={
                "Name": "name",
                "Address": "address",
                "Tel": "phone"
            })

            df.to_csv(RAW_PATH, index=False, encoding="utf-8-sig")
            print(f"📦 已成功抓取 {len(df)} 筆資料，輸出至 {RAW_PATH}")
            return df

        except requests.exceptions.Timeout:
            print(f"⚠️ 第 {attempt} 次嘗試超時，{5 if attempt < max_retries else 0} 秒後重試...")
            if attempt < max_retries:
                time.sleep(5)
            else:
                print("❌ 錯誤：連線超時，請檢查網路或 API 狀態。")
                raise

        except Exception as e:
            print(f"❌ 抓取失敗：{e}")
            if attempt < max_retries:
                print("🔁 準備重試中...")
                time.sleep(5)
            else:
                raise

    print("❌ 錯誤：所有重試均失敗，無法取得資料。")
    return pd.DataFrame()
