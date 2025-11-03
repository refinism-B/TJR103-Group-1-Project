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
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))
from sqlalchemy import create_engine

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

GOOGLE_API_KEY = "AIzaSyAKD_bSB7Z26zBK1JN2yVdTXOxDNEfznQo"
GOOGLE_PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
API_LINK = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=2thVboChxuKs"

data_dir = os.path.join(os.getcwd(), "data")
os.makedirs(data_dir, exist_ok=True)
output_file = os.path.join(data_dir, "taiwan_pet_shelters_with_google.csv")

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
        "fields": "rating,user_ratings_total,opening_hours,current_opening_hours,reviews,url,business_status",
        "language": "zh-TW",
        "key": GOOGLE_API_KEY,
    }
    res = requests.get(GOOGLE_PLACES_DETAILS_URL, params=params, timeout=10)
    data = res.json()
    result = data.get("result", {})

    opening_hours = None
    if result.get("opening_hours") and "weekday_text" in result["opening_hours"]:
        opening_hours = "; ".join(result["opening_hours"]["weekday_text"])

    # ✅ 改用 business_status
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

    gmap_url = result.get("url")

    return {
        "rating": result.get("rating"),
        "user_ratings_total": result.get("user_ratings_total"),
        "opening_hours": opening_hours,
        "business_status": business_status,
        "latest_review_time": latest_review_time,
        "gmap_url": gmap_url
    }

def parse_opening_hours(opening_hours_str):
    if not opening_hours_str or pd.isna(opening_hours_str):
        return None
    total_hours = 0.0
    for day_info in opening_hours_str.split("; "):
        try:
            # 範例: "星期一: 休息" 或 "星期二: 10:00–12:00, 14:00–16:00"
            parts = day_info.split(": ")
            if len(parts) != 2:
                continue

            day_label, time_part = parts[0], parts[1]

            # ✅ 若包含休息、未營業、無資料則略過
            if any(kw in time_part for kw in ["休息", "未營業", "公休", "不營業"]):
                continue

            # ✅ 確保時間區段有 "–"
            time_ranges = [r.strip() for r in time_part.split(",") if "–" in r]
            for time_range in time_ranges:
                try:
                    start_str, end_str = [t.strip() for t in time_range.split("–")]
                    start = datetime.strptime(start_str, "%H:%M")
                    end = datetime.strptime(end_str, "%H:%M")

                    # 若跨午夜，補一天
                    if end < start:
                        end = end.replace(day=start.day + 1)

                    duration = (end - start).seconds / 3600
                    total_hours += duration
                except Exception as inner_e:
                    print(f"⚠️ 無法解析時間段：{day_info} - {inner_e}")
                    continue
        except Exception as e:
            print(f"⚠️ 無法解析時間段：{day_info} - {e}")
            continue

    return round(total_hours, 2)


def enrich_with_google_info(row):
    name, addr = row["收容所名稱"], row["地址"]
    try:
        place_info = get_place_info(name, addr)
        if not place_info:
            print(f"⚠️ 找不到 {name} 的 Google 資料")
            return {
                "Google 評分": None,
                "評分人數": None,
                "營業時間": None,
                "Place ID": None,
                "經度": None,
                "緯度": None,
                "營業狀態": None,
                "最新評論日期": None,
                "GMap 網址": None
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
            "營業狀態": details["business_status"],  # ✅ 改這裡
            "最新評論日期": details["latest_review_time"],
            "GMap 網址": details["gmap_url"]
        }
    except Exception as e:
        print(f"⚠️ 查詢失敗：{name} ({addr}) - {e}")
        return {
            "Google 評分": None,
            "評分人數": None,
            "營業時間": None,
            "Place ID": None,
            "經度": None,
            "緯度": None,
            "營業狀態": None,
            "最新評論日期": None,
            "GMap 網址": None
        }

def override_with_adoption_info(row):
    name = row["收容所名稱"]
    if "苗栗" in name and "收容所" in name:
        opening_hours = "; ".join([
            "星期一: 休息",
            "星期二: 10:00–12:00, 13:00–16:00",
            "星期三: 10:00–12:00, 13:00–16:00",
            "星期四: 10:00–12:00, 13:00–16:00",
            "星期五: 10:00–12:00, 13:00–16:00",
            "星期六: 10:00–12:00, 13:00–16:00",
            "星期日: 休息"
        ])
        return opening_hours, 25.0
    elif "瑞芳" in name:
        opening_hours = "; ".join([
            "星期一: 10:00–12:00, 14:00–16:00",
            "星期二: 10:00–12:00, 14:00–16:00",
            "星期三: 10:00–12:00, 14:00–16:00",
            "星期四: 10:00–12:00, 14:00–16:00",
            "星期五: 10:00–12:00, 14:00–16:00",
            "星期六: 休息",
            "星期日: 休息"
        ])
        return opening_hours, 20.0
    return row["營業時間"], row["每週營業時數"]

def open_file(filepath):
    system = platform.system()
    try:
        if system == "Windows":
            os.startfile(filepath)
        elif system == "Darwin":
            subprocess.call(["open", filepath])
        elif system == "Linux":
            subprocess.call(["xdg-open", filepath])
    except Exception as e:
        print(f"⚠️ 無法自動開啟檔案：{e}")

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

    print("🔍 查詢 Google Maps 評分、評論人數與營業時間中（多執行緒）...")
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(enrich_with_google_info, row): idx for idx, row in df.iterrows()}
        results = {}
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()

    result_df = pd.DataFrame.from_dict(results, orient="index")
    df = pd.concat([df, result_df], axis=1)

    print("🧹 清理地址格式中...")
    df["地址"] = df["地址"].apply(clean_address)

    print("⏱️ 計算每週營業時數中...")
    df["每週營業時數"] = df["營業時間"].apply(parse_opening_hours)

    print("📌 套用動物保護資訊網認領養時間...")
    df[["營業時間", "每週營業時數"]] = df.apply(override_with_adoption_info, axis=1, result_type="expand")

    print("🆕 已加入欄位：營業狀態、最新評論日期、GMap 網址")

    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"📊 已成功輸出至：{output_file}")
    print(f"✅ 共 {len(df)} 筆收容所資料已完成")

    open_file(output_file)
    print("🪟 已自動開啟輸出檔案")

    return df

if __name__ == "__main__":
    df = main()


# === 將資料寫入 MySQL ===
username = os.getenv("MYSQL_USERNAME")
password = os.getenv("MYSQL_PASSWORD")
target_ip = os.getenv("MYSQL_IP")
target_port = int(os.getenv("MYSQL_PORTT"))
db_name = os.getenv("MYSQL_DB_NAME")

engine = create_engine(f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}")

try:
    df.to_sql(name="pet_shelter", con=engine, if_exists="replace", index=False)
    print("✅ 已成功匯入至 MySQL 資料表：pet_shelter")
except Exception as e:
    print(f"❌ 匯入 MySQL 失敗：{e}")


