import os
import re
import time
import platform
import subprocess
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
import urllib3
from sqlalchemy import create_engine
from dotenv import load_dotenv

# === 導入自訂模組 ===
from mods import gmap   # ✅ 直接從專案根目錄引用

# === 初始化環境設定 ===
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

API_LINK = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=2thVboChxuKs"
data_dir = os.path.join(os.getcwd(), "data")
os.makedirs(data_dir, exist_ok=True)
output_file = os.path.join(data_dir, "taiwan_pet_shelters_with_google.csv")

# ✅ 從 .env 取得 API Key 或使用 fallback
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# === 基本函式 ===
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


def parse_opening_hours(opening_hours_str):
    if not opening_hours_str or pd.isna(opening_hours_str):
        return None
    total_hours = 0.0
    for day_info in opening_hours_str:
        try:
            # weekday_text 格式如: "星期二: 上午10:00–下午4:00"
            parts = re.split(r"[:：]", day_info, 1)
            if len(parts) != 2:
                continue

            time_part = parts[1].strip()
            if any(kw in time_part for kw in ["休息", "未營業", "公休", "不營業"]):
                continue

            time_ranges = re.findall(r"(\d{1,2}:\d{2})[–~－\-](\d{1,2}:\d{2})", time_part)
            for start_str, end_str in time_ranges:
                start = datetime.strptime(start_str, "%H:%M")
                end = datetime.strptime(end_str, "%H:%M")
                if end < start:
                    end = end.replace(day=start.day + 1)
                total_hours += (end - start).seconds / 3600
        except Exception as e:
            print(f"⚠️ 無法解析時間段：{day_info} - {e}")
    return round(total_hours, 2)


def override_with_adoption_info(row):
    """用人工設定覆蓋部分收容所時間"""
    name = row["收容所名稱"]
    if "苗栗" in name and "收容所" in name:
        opening_hours = [
            "星期一: 休息",
            "星期二: 10:00–12:00, 13:00–16:00",
            "星期三: 10:00–12:00, 13:00–16:00",
            "星期四: 10:00–12:00, 13:00–16:00",
            "星期五: 10:00–12:00, 13:00–16:00",
            "星期六: 10:00–12:00, 13:00–16:00",
            "星期日: 休息",
        ]
        return "; ".join(opening_hours), 25.0
    elif "瑞芳" in name:
        opening_hours = [
            "星期一: 10:00–12:00, 14:00–16:00",
            "星期二: 10:00–12:00, 14:00–16:00",
            "星期三: 10:00–12:00, 14:00–16:00",
            "星期四: 10:00–12:00, 14:00–16:00",
            "星期五: 10:00–12:00, 14:00–16:00",
            "星期六: 休息",
            "星期日: 休息",
        ]
        return "; ".join(opening_hours), 20.0
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


# === 使用 mods/gmap 模組整合 ===
def enrich_with_google_info(row):
    """透過 gmap 模組查詢 Google Maps 資訊"""
    name, addr = row["收容所名稱"], row["地址"]
    try:
        place_dict = gmap.get_place_dict(
            name=name,
            address=addr,
            api_key=GOOGLE_API_KEY
        )
        if not place_dict or not place_dict["place_id"]:
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

        print(f"✅ {name} → {place_dict['rating']}⭐ ({place_dict['rating_total']} 則)")
        return {
            "Google 評分": place_dict["rating"],
            "評分人數": place_dict["rating_total"],
            "營業時間": "; ".join(place_dict["opening_hours"]) if place_dict["opening_hours"] else None,
            "Place ID": place_dict["place_id"],
            "經度": place_dict["longitude"],
            "緯度": place_dict["latitude"],
            "營業狀態": place_dict["business_status"],
            "最新評論日期": place_dict["newest_review"],
            "GMap 網址": place_dict["map_url"],
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
    return df


if __name__ == "__main__":
    df = main()

    # === 將資料寫入 MySQL ===
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = int(os.getenv("MYSQL_PORTT", "3306"))
    db_name = os.getenv("MYSQL_DB_NAME")

    try:
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}")
        df.to_sql(name="pet_shelter", con=engine, if_exists="replace", index=False)
        print("✅ 已成功匯入至 MySQL 資料表：pet_shelter")
    except Exception as e:
        print(f"❌ 匯入 MySQL 失敗：{e}")
