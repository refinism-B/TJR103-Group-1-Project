import os
import requests
import pandas as pd
from time import sleep
from datetime import datetime

GOOGLE_API_KEY = "AIzaSyAKD_bSB7Z26zBK1JN2yVdTXOxDNEfznQo"
PLACES_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

cities = ["台北市", "新北市", "桃園市", "台中市", "台南市", "高雄市"]
query_template = "{} 寵物美容"
all_data = []

def search_places(query):
    results = []
    params = {
        "query": query,
        "key": GOOGLE_API_KEY,
        "language": "zh-TW"
    }
    while True:
        res = requests.get(PLACES_URL, params=params)
        data = res.json()
        results.extend(data.get("results", []))
        next_page_token = data.get("next_page_token")
        if not next_page_token:
            break
        sleep(2)
        params["pagetoken"] = next_page_token
    return results

def get_place_details(place_id):
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,rating,user_ratings_total,business_status,url,formatted_phone_number,opening_hours,reviews",
        "key": GOOGLE_API_KEY,
        "language": "zh-TW"
    }
    res = requests.get(DETAILS_URL, params=params)
    return res.json().get("result", {})

def extract_district(address):
    for part in address.split():
        if any(kw in part for kw in ["區", "鎮", "鄉"]):
            return part
    return None

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
                if "休息" in time_range or "未營業" in time_range:
                    continue  # ✅ 跳過休息日
                if "–" not in time_range:
                    continue  # ✅ 跳過無法解析的格式
                start_str, end_str = time_range.split("–")
                start = datetime.strptime(start_str.strip(), "%H:%M")
                end = datetime.strptime(end_str.strip(), "%H:%M")
                duration = (end - start).seconds / 3600
                total_hours += duration
        except Exception as e:
            print(f"⚠️ 無法解析時間段：{day_info} - {e}")
            continue
    return round(total_hours, 2)

def get_latest_review_time(reviews):
    timestamps = [r.get("time") for r in reviews if r.get("time")]
    if timestamps:
        return datetime.fromtimestamp(max(timestamps)).strftime("%Y-%m-%d")
    return None

for city in cities:
    print(f"🔍 查詢中：{city}")
    query = query_template.format(city)
    places = search_places(query)

    for place in places:
        place_id = place.get("place_id")
        details = get_place_details(place_id)
        sleep(1)

        address = details.get("formatted_address", "")
        district = extract_district(address)
        phone = details.get("formatted_phone_number")
        hours = details.get("opening_hours", {}).get("weekday_text")
        hours_str = "; ".join(hours) if hours else None
        weekly_hours = parse_opening_hours(hours_str)
        latest_review = get_latest_review_time(details.get("reviews", []))

        status_map = {
            "OPERATIONAL": "營業中",
            "CLOSED_TEMPORARILY": "暫時關閉",
            "CLOSED_PERMANENTLY": "永久停業"
        }

        all_data.append({
            "縣市": city,
            "行政區": district,
            "店名": details.get("name"),
            "地址": address,
            "電話": phone,
            "Google 評分": details.get("rating"),
            "評分人數": details.get("user_ratings_total"),
            "營業狀態": status_map.get(details.get("business_status")),
            "營業時間": hours_str,
            "每週營業時數": weekly_hours,
            "最新評論日期": latest_review,
            "GMap 網址": details.get("url")
        })

# 儲存到 data 資料夾
data_dir = os.path.join(os.getcwd(), "data")
os.makedirs(data_dir, exist_ok=True)
output_path = os.path.join(data_dir, "six_city_pet_grooming_google.csv")

df = pd.DataFrame(all_data)
df.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"✅ 已儲存至：{output_path}")