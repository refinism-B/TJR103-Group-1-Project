import sys, os, re, pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(ROOT_DIR)

from mods import gmap as gm
from config import API_KEY

def clean_address(address):
    if pd.isna(address):
        return address
    address = re.sub(r"^\d{3,5}", "", address)
    address = re.sub(r"[\(（][^\)）]*[\)）]", "", address)
    return address.strip()

def parse_opening_hours(opening_hours_list):
    if not opening_hours_list:
        return None
    text = "; ".join(opening_hours_list) if isinstance(opening_hours_list, list) else str(opening_hours_list)
    total_hours = 0
    for day_info in text.split("; "):
        try:
            if ":" not in day_info:
                continue
            _, time_part = day_info.split(": ", 1)
            if any(kw in time_part for kw in ["休息", "未營業", "公休", "不營業"]):
                continue
            for time_range in [r.strip() for r in time_part.split(",") if "–" in r]:
                start, end = [datetime.strptime(t.strip(), "%H:%M") for t in time_range.split("–")]
                if end < start:
                    end = end.replace(day=start.day + 1)
                total_hours += (end - start).seconds / 3600
        except:
            continue
    return round(total_hours, 2)

def enrich_with_google_info(row):
    """查詢 Google Maps 地標資訊"""
    name, addr = row["收容所名稱"], row["地址"]
    try:
        place = gm.get_place_dict(name=name, address=addr, api_key=API_KEY)
        if not place:
            print(f"⚠️ 找不到 {name} 的 Google 資料")
            return {k: None for k in ["Google 評分","評分人數","營業時間","Place ID","經度","緯度","營業狀態","最新評論日期","GMap 網址"]}
        print(f"✅ {name} → {place.get('rating')}⭐ ({place.get('rating_total')} 則)")
        return {
            "Google 評分": place.get("rating"),
            "評分人數": place.get("rating_total"),
            "營業時間": "; ".join(place["opening_hours"]) if place["opening_hours"] else None,
            "Place ID": place.get("place_id"),
            "經度": place.get("longitude"),
            "緯度": place.get("latitude"),
            "營業狀態": place.get("business_status"),
            "最新評論日期": place.get("newest_review"),
            "GMap 網址": place.get("map_url"),
        }
    except Exception as e:
        print(f"⚠️ 查詢失敗：{name} ({addr}) - {e}")
        return {k: None for k in ["Google 評分","評分人數","營業時間","Place ID","經度","緯度","營業狀態","最新評論日期","GMap 網址"]}

def transform_shelter_data(df):
    df["地址"] = df["地址"].apply(clean_address)
    print("🔍 查詢 Google Maps 評分與營業時間（多執行緒）...")
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(enrich_with_google_info, row): idx for idx, row in df.iterrows()}
        results = {idx: f.result() for f, idx in zip(as_completed(futures), futures.values())}
    enriched = pd.DataFrame.from_dict(results, orient="index")
    df = pd.concat([df, enriched], axis=1)
    df["每週營業時數"] = df["營業時間"].apply(parse_opening_hours)
    return df
