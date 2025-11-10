import os
import requests
import pandas as pd
import urllib3
import platform
import subprocess
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === API 設定 ===
GOOGLE_API_KEY = "AIzaSyDLALYT1HVtKl3KD2s18IH8IB-trnDszmo"
GOOGLE_PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
API_LINK = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=2thVboChxuKs"

# === 輸出目錄 ===
data_dir = os.path.join(os.getcwd(), "data")
os.makedirs(data_dir, exist_ok=True)
output_file = os.path.join(data_dir, "taiwan_pet_shelters_with_google.csv")


# === 抓取農業部 API 資料 ===
def get_api_json(url: str):
    res = requests.get(url, verify=False, timeout=15)
    res.raise_for_status()
    return res.json()


def clean_address(address):
    if pd.isna(address):
        return address
    address = re.sub(r"^\d{3,5}", "", address).strip()
    address = re.sub(r"[\(（][^\)）]*[\)）]", "", address)
    return address.strip()


# === Google Maps 查詢 ===
def get_place_info(name, address):
    query = f"{name} {address}"
    params = {"query": query, "key": GOOGLE_API_KEY, "language": "zh-TW"}
    res = requests.get(GOOGLE_PLACES_SEARCH_URL, params=params, timeout=10)
    data = res.json()
    if data.get("results"):
        result = data["results"][0]
        return {
            "place_id": result.get("place_id"),
            "lat": result["geometry"]["location"]["lat"],
            "lng": result["geometry"]["location"]["lng"]
        }
    return None


def get_place_details(place_id):
    params = {
        "place_id": place_id,
        "fields": "rating,user_ratings_total,opening_hours,reviews,url,business_status",
        "language": "zh-TW",
        "key": GOOGLE_API_KEY,
    }
    res = requests.get(GOOGLE_PLACES_DETAILS_URL, params=params, timeout=10)
    data = res.json()
    result = data.get("result", {})

    opening_hours = None
    if result.get("opening_hours") and "weekday_text" in result["opening_hours"]:
        opening_hours = "; ".join(result["opening_hours"]["weekday_text"])

    business_status_map = {
        "OPERATIONAL": "營業中",
        "CLOSED_TEMPORARILY": "暫時關閉",
        "CLOSED_PERMANENTLY": "永久停業"
    }
    business_status = business_status_map.get(result.get("business_status"), None)

    latest_review_time = None
    if result.get("reviews"):
        timestamps = [r.get("time") for r in result["reviews"] if r.get("time")]
        if timestamps:
            latest_review_time = datetime.fromtimestamp(max(timestamps)).strftime("%Y-%m-%d")

    return {
        "rating": result.get("rating"),
        "user_ratings_total": result.get("user_ratings_total"),
        "opening_hours": opening_hours,
        "business_status": business_status,
        "latest_review_time": latest_review_time,
        "gmap_url": result.get("url")
    }


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


# === Google 多執行緒查詢 ===
def enrich_with_google_info(row):
    name, addr = row["收容所名稱"], row["地址"]
    try:
        place_info = get_place_info(name, addr)
        if not place_info:
            print(f"⚠️ 找不到 {name} 的 Google 資料")
            return {
                "Google 評分": None, "評分人數": None, "營業時間": None,
                "Place ID": None, "經度": None, "緯度": None,
                "營業狀態": None, "最新評論日期": None, "GMap 網址": None
            }

        details = get_place_details(place_info["place_id"])
        print(f"✅ {name} → {details['rating']}⭐ ({details['user_ratings_total']} 則)")
        return {
            "Google 評分": details["rating"],
            "評分人數": details["user_ratings_total"],
            "營業時間": details["opening_hours"],
            "Place ID": place_info["place_id"],
            "經度": place_info["lng"],
            "緯度": place_info["lat"],
            "營業狀態": details["business_status"],
            "最新評論日期": details["latest_review_time"],
            "GMap 網址": details["gmap_url"]
        }
    except Exception as e:
        print(f"⚠️ 查詢失敗：{name} ({addr}) - {e}")
        return {
            "Google 評分": None, "評分人數": None, "營業時間": None,
            "Place ID": None, "經度": None, "緯度": None,
            "營業狀態": None, "最新評論日期": None, "GMap 網址": None
        }


# === 主流程 ===
def main():
    print("🐾 正在從農業部 API 抓取資料...")
    data = get_api_json(API_LINK)
    df = pd.DataFrame(data)
    print(f"📋 共取得 {len(df)} 筆全台收容所資料")

    df = df[["ShelterName", "CityName", "Address", "Phone"]].copy()
    df.rename(columns={
        "ShelterName": "收容所名稱",
        "CityName": "縣市",
        "Address": "地址",
        "Phone": "電話",
    }, inplace=True)

    print("🔍 查詢 Google Maps 資料中（多執行緒）...")
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(enrich_with_google_info, row): idx for idx, row in df.iterrows()}
        results = {}
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()

    df = pd.concat([df, pd.DataFrame.from_dict(results, orient="index")], axis=1)

    df["地址"] = df["地址"].apply(clean_address)
    df["每週營業時數"] = df["營業時間"].apply(parse_opening_hours)

    print("🧩 欄位統一與排序中...")

    df.rename(columns={
        "收容所名稱": "name",
        "縣市": "loc_id",
        "地址": "address",
        "電話": "phone",
        "營業狀態": "buss_status",
        "每週營業時數": "op_hours",
        "Google 評分": "rating",
        "評分人數": "rating_total",
        "最新評論日期": "newest_review",
        "經度": "longitude",
        "緯度": "latitude",
        "GMap 網址": "map_url",
        "Place ID": "place_id"
    }, inplace=True)

    df["loc_id"] = [f"SH{str(i+1).zfill(4)}" for i in range(len(df))]
    df["category_id"] = 1
    df["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df = df[[
        "name", "buss_status", "loc_id", "address", "phone",
        "op_hours", "category_id", "rating", "rating_total",
        "newest_review", "longitude", "latitude", "map_url",
        "place_id", "update_time"
    ]]

    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"📊 已成功輸出：{output_file}")
    print(f"✅ 共 {len(df)} 筆資料完成")
    return df


if __name__ == "__main__":
    df = main()

    # === 匯入 MySQL ===
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = int(os.getenv("MYSQL_PORTT"))
    db_name = os.getenv("MYSQL_DB_NAME")

    engine = create_engine(f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}")
    try:
        df.to_sql(name="pet_shelter", con=engine, if_exists="replace", index=False)
        print("✅ 已成功匯入 MySQL 資料表：pet_shelter")
    except Exception as e:
        print(f"❌ 匯入 MySQL 失敗：{e}")
