import os
import requests
import pandas as pd
import urllib3
import platform
import subprocess
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 關閉 HTTPS 驗證警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# === Google API 設定 ===
GOOGLE_API_KEY = "AIzaSyAKD_bSB7Z26zBK1JN2yVdTXOxDNEfznQo"
GOOGLE_PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_PLACES_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# === 農業部開放資料 API ===
API_LINK = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=2thVboChxuKs"

# === 輸出設定 ===
data_dir = os.path.join(os.getcwd(), "data")
os.makedirs(data_dir, exist_ok=True)
output_file = os.path.join(data_dir, "taiwan_pet_shelters_with_google.csv")

# === 抓農業部資料 ===
def get_api_json(url: str):
    res = requests.get(url, verify=False, timeout=15)
    res.raise_for_status()
    return res.json()

def clean_address(address):
    if pd.isna(address):
        return address
    # 去除開頭的郵遞區號（3~5位數字）
    address = re.sub(r"^\d{3,5}", "", address).strip()
    # 移除中英文括號及其內容
    address = re.sub(r"[\(（][^\)）]*[\)）]", "", address)
    # 去除多餘空白
    return address.strip()

# === 查 Google Place ID ===
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

# === 查 Google 詳細資訊（評分、評論人數、營業時間） ===
def get_place_details(place_id):
    params = {
        "place_id": place_id,
        "fields": "rating,user_ratings_total,opening_hours",
        "language": "zh-TW",
        "key": GOOGLE_API_KEY,
    }
    res = requests.get(GOOGLE_PLACES_DETAILS_URL, params=params, timeout=10)
    data = res.json()
    result = data.get("result", {})

    opening_hours = None
    if result.get("opening_hours") and "weekday_text" in result["opening_hours"]:
        opening_hours = "; ".join(result["opening_hours"]["weekday_text"])

    return {
        "rating": result.get("rating"),
        "user_ratings_total": result.get("user_ratings_total"),
        "opening_hours": opening_hours,
    }

# === 計算每週營業時數 ===
def parse_opening_hours(opening_hours_str):
    if not opening_hours_str or pd.isna(opening_hours_str):
        return None

    total_hours = 0.0
    for day_info in opening_hours_str.split("; "):
        try:
            parts = day_info.split(": ")
            if len(parts) != 2:
                continue
            time_ranges = parts[1].split(", ")
            for time_range in time_ranges:
                start_str, end_str = time_range.split("–")
                start = datetime.strptime(start_str.strip(), "%H:%M")
                end = datetime.strptime(end_str.strip(), "%H:%M")
                duration = (end - start).seconds / 3600
                total_hours += duration
        except Exception as e:
            print(f"⚠️ 無法解析時間段：{day_info} - {e}")
            continue

    return round(total_hours, 2)

# === 整合查詢邏輯 ===
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
                "緯度": None
            }

        details = get_place_details(place_info["place_id"])
        print(f"✅ {name} → {details['rating']}⭐ ({details['user_ratings_total']} 則)")
        return {
            "Google 評分": details["rating"],
            "評分人數": details["user_ratings_total"],
            "營業時間": details["opening_hours"],
            "Place ID": place_info["place_id"],
            "經度": place_info["lng"],
            "緯度": place_info["lat"]
        }
    except Exception as e:
        print(f"⚠️ 查詢失敗：{name} ({addr}) - {e}")
        return {
            "Google 評分": None,
            "評分人數": None,
            "營業時間": None,
            "Place ID": None,
            "經度": None,
            "緯度": None
        }

# === 覆蓋指定收容所的營業時間與時數為動物保護資訊網資料 ===
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

# === 自動開啟檔案 ===
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

# === 主程式 ===
def main():
    print("🐾 正在從農業部 API 抓取資料...")
    data = get_api_json(API_LINK)
    df = pd.DataFrame(data)
    print(f"📋 共取得 {len(df)} 筆全台收容所資料")

    # 篩選欄位
    df = df[["ShelterName", "CityName", "Address", "Phone"]].copy()
    df.rename(columns={
        "ShelterName": "收容所名稱",
        "CityName": "縣市",
        "Address": "地址",
        "Phone": "電話",
    }, inplace=True)

    # 多執行緒查詢 Google Maps
    print("🔍 查詢 Google Maps 評分、評論人數與營業時間中（多執行緒）...")
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(enrich_with_google_info, row): idx for idx, row in df.iterrows()}
        results = {}
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()

    # 合併結果
    result_df = pd.DataFrame.from_dict(results, orient="index")
    df = pd.concat([df, result_df], axis=1)

    # 清理地址格式
    print("🧹 清理地址格式中...")
    df["地址"] = df["地址"].apply(clean_address)

    # 計算每週營業時數
    print("⏱️ 計算每週營業時數中...")
    df["每週營業時數"] = df["營業時間"].apply(parse_opening_hours)

    # 覆蓋指定收容所的營業時間與時數
    print("📌 套用動物保護資訊網認領養時間...")
    df[["營業時間", "每週營業時數"]] = df.apply(override_with_adoption_info, axis=1, result_type="expand")

    # 匯出 CSV
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"📊 已成功輸出至：{output_file}")
    print(f"✅ 共 {len(df)} 筆收容所資料已完成")

    # 自動開啟 CSV
    open_file(output_file)
    print("🪟 已自動開啟輸出檔案")

if __name__ == "__main__":
    main()

    