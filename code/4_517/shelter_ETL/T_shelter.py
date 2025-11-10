import os
import re
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

# === API 設定 ===
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or "請填入你的Google API金鑰"
GOOGLE_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# === 檔案設定 ===
PROCESSED_DIR = os.path.join(os.getcwd(), "data", "processed", "shelter")
os.makedirs(PROCESSED_DIR, exist_ok=True)
PROCESSED_PATH = os.path.join(PROCESSED_DIR, "shelter_processed.csv")


# === 取得 Google Place 資訊 ===
def get_google_info(name, address):
    try:
        params = {"query": f"{name} {address}", "key": GOOGLE_API_KEY, "language": "zh-TW"}
        search = requests.get(GOOGLE_SEARCH_URL, params=params, timeout=10)
        data = search.json()
        if not data.get("results"):
            return None

        result = data["results"][0]
        place_id = result.get("place_id")

        # 呼叫 Place Details API
        details_params = {
            "place_id": place_id,
            "fields": "rating,user_ratings_total,opening_hours,url,website,business_status,geometry",
            "language": "zh-TW",
            "key": GOOGLE_API_KEY
        }
        details = requests.get(GOOGLE_DETAILS_URL, params=details_params, timeout=10).json().get("result", {})

        # 營業時間轉換
        opening_hours = None
        if details.get("opening_hours") and "weekday_text" in details["opening_hours"]:
            opening_hours = "; ".join(details["opening_hours"]["weekday_text"])

        return {
            "buss_status": details.get("business_status", "OPERATIONAL"),
            "rating": details.get("rating"),
            "rating_total": details.get("user_ratings_total"),
            "opening_hours": opening_hours,
            "longitude": details.get("geometry", {}).get("location", {}).get("lng"),
            "latitude": details.get("geometry", {}).get("location", {}).get("lat"),
            "map_url": details.get("url"),
            "website": details.get("website", ""),
            "place_id": place_id
        }
    except Exception:
        return None


# === 每週營業時數計算 ===
def parse_opening_hours(opening_hours_str):
    if not opening_hours_str or pd.isna(opening_hours_str):
        return None
    total_hours = 0.0
    for day_info in opening_hours_str.split("; "):
        try:
            parts = day_info.split(": ")
            if len(parts) != 2:
                continue
            time_part = parts[1]
            if any(kw in time_part for kw in ["休息", "未營業", "公休", "不營業"]):
                continue
            time_ranges = [r.strip() for r in time_part.split(",") if "–" in r]
            for time_range in time_ranges:
                start_str, end_str = [t.strip() for t in time_range.split("–")]
                start = datetime.strptime(start_str, "%H:%M")
                end = datetime.strptime(end_str, "%H:%M")
                if end < start:
                    end = end.replace(day=start.day + 1)
                total_hours += (end - start).seconds / 3600
        except Exception:
            continue
    return round(total_hours, 2)


# === 地址清理 ===
def clean_address(address):
    if pd.isna(address):
        return address
    addr = re.sub(r"^\d{3,5}", "", str(address))
    addr = re.sub(r"[\(（][^\)）]*[\)）]", "", addr)
    return addr.strip()


# === 主轉換邏輯 ===
def transform(df):
    print("⚙️ 開始資料轉換與清理...")

    df["address"] = df["address"].apply(clean_address)

    # Google Maps 多執行緒查詢
    print("🌍 正在查詢 Google Maps 詳細資料...")
    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(get_google_info, row["name"], row["address"]): idx for idx, row in df.iterrows()}
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result() or {}

    gdf = pd.DataFrame.from_dict(results, orient="index")
    df = pd.concat([df, gdf], axis=1)

    # 每週營業時數
    df["op_hours"] = df["opening_hours"].apply(parse_opening_hours)

    # 基本欄位補齊
    df["id"] = [f"sh{str(i+1).zfill(4)}" for i in range(len(df))]
    df["category_id"] = 6
    df["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 欄位順序
    df = df[[
        "id", "name", "buss_status", "address", "phone",
        "op_hours", "category_id", "rating", "rating_total",
        "opening_hours", "longitude", "latitude", "map_url",
        "website", "place_id", "update_time"
    ]]

    df.to_csv(PROCESSED_PATH, index=False, encoding="utf-8-sig")
    print(f"📊 已輸出：{PROCESSED_PATH}")
    return df
