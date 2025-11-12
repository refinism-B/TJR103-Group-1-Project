# T_shelter.py
import os
import re
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

# === Google Maps API 設定 ===
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY") or "請填入你的Google API金鑰"
GOOGLE_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

# === 檔案設定 ===
PROCESSED_DIR = os.path.join(os.getcwd(), "data", "processed", "shelter")
os.makedirs(PROCESSED_DIR, exist_ok=True)
PROCESSED_PATH = os.path.join(PROCESSED_DIR, "shelter_processed.csv")

# === 六都對應表 ===
CITY_LOC_MAP = {
    "新北市": "NTP", "臺北市": "TPE", "桃園市": "TYN",
    "臺中市": "TCH", "臺南市": "TNA", "高雄市": "KSH"
}


# === Google Maps 查詢 ===
def get_google_info(name, address):
    try:
        params = {"query": f"{name} {address}", "key": GOOGLE_API_KEY, "language": "zh-TW"}
        search = requests.get(GOOGLE_SEARCH_URL, params=params, timeout=10)
        data = search.json()
        if not data.get("results"):
            return None

        result = data["results"][0]
        place_id = result.get("place_id")

        details_params = {
            "place_id": place_id,
            "fields": "rating,user_ratings_total,opening_hours,url,website,"
                      "business_status,geometry,reviews",
            "language": "zh-TW",
            "key": GOOGLE_API_KEY,
        }
        details = requests.get(GOOGLE_DETAILS_URL, params=details_params, timeout=10).json().get("result", {})

        opening_hours = None
        if details.get("opening_hours") and "weekday_text" in details["opening_hours"]:
            opening_hours = "; ".join(details["opening_hours"]["weekday_text"])

        newest_review = ""
        if "reviews" in details and details["reviews"]:
            review = details["reviews"][0]
            time_str = datetime.fromtimestamp(review["time"]).strftime("%Y-%m-%d")
            text = review.get("text", "").replace("\n", " ").strip()
            newest_review = f"[{time_str}] {text}"

        return {
            "buss_status": details.get("business_status", "OPERATIONAL"),
            "rating": details.get("rating"),
            "rating_total": details.get("user_ratings_total"),
            "opening_hours": opening_hours,
            "longitude": details.get("geometry", {}).get("location", {}).get("lng"),
            "latitude": details.get("geometry", {}).get("location", {}).get("lat"),
            "map_url": details.get("url"),
            "website": details.get("website", ""),
            "place_id": place_id,
            "newest_review": newest_review
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
    address = re.sub(r"^\d{3,5}", "", str(address))  # 移除郵遞區號
    address = re.sub(r"[\(（][^\)）]*[\)）]", "", address)  # 去掉括號內容
    return address.strip()


# === 官方網站修正 ===
SPECIAL_WEBSITES = [
    (re.compile(r"(基隆|基隆市)"), "https://www.klaphio.klcg.gov.tw/tw/klaphio/1326.html"),
]

def pick_official_website(name, address, website):
    w = (website or "").strip().lower()
    if any(bad in w for bad in [
        "facebook.com",
        "asms.coa.gov.tw",
        "animal.coa.gov.tw",
        "animal-adoption.coa.gov.tw",
        "adopt.coa.gov.tw"
    ]):
        w = ""
    for pattern, url in SPECIAL_WEBSITES:
        if pattern.search(name) or pattern.search(address):
            return url
    return w


# === 主轉換流程 ===
def transform(df):
    print("⚙️ [T] Transform - 開始資料清理與整合...")

    df["address"] = df["address"].apply(clean_address)

    print("🌍 查詢 Google Maps 資料中...")
    results = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(get_google_info, row["name"], row["address"]): idx for idx, row in df.iterrows()}
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result() or {}

    gdf = pd.DataFrame.from_dict(results, orient="index")
    df = pd.concat([df, gdf], axis=1)

    def get_loc_id(addr):
        for city, prefix in CITY_LOC_MAP.items():
            if city in str(addr):
                return prefix + "000"
        return None

    df["loc_id"] = df["address"].apply(get_loc_id)
    df["website"] = df.apply(lambda r: pick_official_website(r["name"], r["address"], r.get("website", "")), axis=1)
    if "opening_hours" in df.columns:
        df["op_hours"] = df["opening_hours"].apply(parse_opening_hours)
    else:
        df["op_hours"] = None

    # === 🆕 只保留 newest_review 日期 ===
    if "newest_review" in df.columns:
        df["newest_review"] = df["newest_review"].apply(
            lambda x: re.sub(r"\s+", " ", x) if isinstance(x, str) else x
        )
    else:
        df["newest_review"] = None

    print("🏙️ 過濾六都資料並重新編號中...")

    # === 🔍 只保留六都資料 ===
    six_city_keywords = list(CITY_LOC_MAP.keys())
    df = df[df["address"].apply(lambda x: any(city in str(x) for city in six_city_keywords))].reset_index(drop=True)

    # === 🔢 重新編號 ID ===
    df["id"] = [f"sh{str(i+1).zfill(4)}" for i in range(len(df))]
    df["category_id"] = 6
    df["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # === 欄位順序 ===
    df = df[[
        "id", "name", "buss_status", "loc_id", "address", "phone",
        "op_hours", "category_id", "rating", "rating_total",
        "newest_review", "longitude", "latitude", "map_url",
        "website", "place_id", "update_time"
    ]]

    df.to_csv(PROCESSED_PATH, index=False, encoding="utf-8-sig")
    print(f"📊 已輸出六都資料：{PROCESSED_PATH}")
    print(f"✅ 六都資料筆數：{len(df)}")
    return df
