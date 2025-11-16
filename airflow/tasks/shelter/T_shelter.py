import os
import re
import requests
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


def get_engine():
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    ip = os.getenv("MYSQL_IP")
    port = os.getenv("MYSQL_PORT")
    db = os.getenv("MYSQL_DB_NAME")
    return create_engine(f"mysql+pymysql://{username}:{password}@{ip}:{port}/{db}")


def standardize_columns(df):
    return df.rename(columns={
        "Name": "name", "ShelterName": "name", "收容所名稱": "name",
        "Address": "address", "地址": "address",
        "Tel": "phone", "Phone": "phone", "電話": "phone"
    })


def clean_address(address):
    address = re.sub(r"^\d{3,5}", "", str(address))
    address = re.sub(r"[\(（][^\)）]*[\)）]", "", address)
    return address.strip()


def clean_name(name):
    """移除半形/全形括弧及括弧內的文字"""
    if not isinstance(name, str):
        return name
    name = re.sub(r"[\(（].*?[\)）]", "", name)
    return name.strip()


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


def get_google_info(name, address):
    try:
        params = {"query": f"{name} {address}", "key": GOOGLE_API_KEY, "language": "zh-TW"}
        search = requests.get(GOOGLE_SEARCH_URL, params=params, timeout=10).json()
        if not search.get("results"):
            return None
        result = search["results"][0]
        place_id = result.get("place_id")

        details_params = {
            "place_id": place_id,
            "fields": "rating,user_ratings_total,opening_hours,url,website,"
                      "business_status,geometry,reviews",
            "language": "zh-TW",
            "key": GOOGLE_API_KEY,
        }
        details = requests.get(GOOGLE_DETAILS_URL, params=details_params, timeout=10).json().get("result", {})

        opening_hours = "; ".join(details.get("opening_hours", {}).get("weekday_text", []))
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


def extract_city_district(address):
    match = re.match(r"(臺北市|新北市|桃園市|臺中市|臺南市|高雄市)(\S+區)", str(address))
    return match.groups() if match else (None, None)


def get_loc_id_from_db(city, district):
    if not city or not district:
        return None
    engine = get_engine()
    query = text("SELECT loc_id FROM location WHERE city = :city AND district = :district LIMIT 1")
    with engine.connect() as conn:
        result = conn.execute(query, {"city": city, "district": district}).fetchone()
        return result[0] if result else None


def transform(df):
    print("⚙️ [T] Transform - 開始資料清理與整合...")
    df = standardize_columns(df)

    # ================================
    # ⭐ 移除括弧內容（新增的部分）
    # ================================
    df["name"] = df["name"].apply(clean_name)

    print("📋 傳入 transform() 的欄位：", df.columns.tolist())

    required_cols = ["name", "address"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"❌ 缺少必要欄位：{missing}")

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

    df["city"], df["district"] = zip(*df["address"].apply(extract_city_district))
    df["loc_id"] = df.apply(lambda r: get_loc_id_from_db(r["city"], r["district"]), axis=1)
    df["op_hours"] = df["opening_hours"].apply(parse_opening_hours)

    df["newest_review"] = df["newest_review"].apply(
        lambda x: re.search(r"\d{4}-\d{2}-\d{2}", str(x)).group(0)
        if re.search(r"\d{4}-\d{2}-\d{2}", str(x))
        else None
    )

    df = df[df["loc_id"].notna()].reset_index(drop=True)
    print(f"✅ loc_id 對應成功筆數：{len(df)}")

    df["id"] = [f"sh{str(i+1).zfill(4)}" for i in range(len(df))]
    df["category_id"] = 6
    df["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df["name"] = df["name"].str.replace("（整修中）", "", regex=False).str.strip()


    df = df[[
        "id", "name", "buss_status", "loc_id", "address", "phone",
        "op_hours", "category_id", "rating", "rating_total",
        "newest_review", "longitude", "latitude", "map_url",
        "website", "place_id", "update_time"
    ]]

    output_paths = [
        os.path.join(os.getcwd(), "TJR103GROUP1", "airflow", "data", "processed", "shelter", "shelter_processed.csv"),
        "/opt/airflow/data/data/complete/store/type=shelter/store.csv"
    ]

    for path in output_paths:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"📊 已輸出資料至：{path}")

    return df
