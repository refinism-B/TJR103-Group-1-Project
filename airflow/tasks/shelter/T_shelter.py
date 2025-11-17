import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from utils.extractdata import (
    cat_id,
    clean_sort,
    create_id,
    extract_city_district_from_df,
    gdata_info,
    gdata_place_id,
    merge_loc,
    to_sql_data,
)

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
GOOGLE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"

host = os.getenv("MYSQL_IP")
user = os.getenv("MYSQL_USERNAME")
password = os.getenv("MYSQL_PASSWORD")
database = os.getenv("MYSQL_DB_NAME")
port = int(os.getenv("MYSQL_PORTT"))
charset = "utf8mb4"


def get_engine():
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    ip = os.getenv("MYSQL_IP")
    port = int(os.getenv("MYSQL_PORTT"))
    db = os.getenv("MYSQL_DB_NAME")
    return create_engine(f"mysql+pymysql://{username}:{password}@{ip}:{port}/{db}")


def standardize_columns(df):
    return df.rename(
        columns={
            "Name": "name",
            "ShelterName": "name",
            "收容所名稱": "name",
            "Address": "address",
            "地址": "address",
            "Tel": "phone",
            "Phone": "phone",
            "電話": "phone",
        }
    )


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
        params = {
            "query": f"{name} {address}",
            "key": GOOGLE_API_KEY,
            "language": "zh-TW",
        }
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
        details = (
            requests.get(GOOGLE_DETAILS_URL, params=details_params, timeout=10)
            .json()
            .get("result", {})
        )

        opening_hours = "; ".join(
            details.get("opening_hours", {}).get("weekday_text", [])
        )
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
            "newest_review": newest_review,
        }
    except Exception:
        return None


def extract_city_district(address):
    match = re.match(
        r"(臺北市|新北市|桃園市|臺中市|臺南市|高雄市)(\S+區)", str(address)
    )
    return match.groups() if match else (None, None)


def get_loc_id_from_db(city, district):
    if not city or not district:
        return None
    engine = get_engine()
    query = text(
        "SELECT loc_id FROM location WHERE city = :city AND district = :district LIMIT 1"
    )
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

    df = df[["name", "address"]]

    df = extract_city_district_from_df(df, "address")
    df = gdata_place_id(
        df,
        GOOGLE_API_KEY,
        "/opt/airflow/data/processed/shelter/shelter_data_place_id.csv",
    )
    df = gdata_info(
        df,
        GOOGLE_API_KEY,
        "/opt/airflow/data/processed/shelter/shelter_data_details.csv",
    )
    df = clean_sort(df, "/opt/airflow/data/processed/shelter/shelter_data_cleaned.csv")
    df = create_id(df, "sh", "/opt/airflow/data/processed/shelter/shelter_data_id.csv")
    df = merge_loc(
        df,
        host,
        port,
        user,
        password,
        database,
        "/opt/airflow/data/processed/shelter/shelter_data_loc_id.csv",
    )
    df = cat_id(
        df,
        host,
        port,
        user,
        password,
        database,
        "/opt/airflow/data/processed/shelter/shelter_data_cat_id.csv",
        "shelter",
    )
    df = to_sql_data(df, "/opt/airflow/data/processed/shelter/shelter_data_sql.csv")

    output_paths = "/opt/airflow/data/data/complete/store/type=shelter/store.csv"

    os.makedirs(os.path.dirname(output_paths), exist_ok=True)
    df.to_csv(output_paths, index=False, encoding="utf-8-sig")
    print(f"📊 已輸出資料至：{output_paths}")

    return df
