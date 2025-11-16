# T_population.py
import os
import re
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

# === 檔案設定 ===
PROCESSED_DIR = os.path.join(os.getcwd(), "data", "processed", "population")
os.makedirs(PROCESSED_DIR, exist_ok=True)
PROCESSED_PATH = os.path.join(PROCESSED_DIR, "population_processed.csv")

# === 六都對應表（用於篩選） ===
CITY_LOC_MAP = {
    "新北市": "NTP", "臺北市": "TPE", "桃園市": "TYN",
    "臺中市": "TCH", "臺南市": "TNA", "高雄市": "KSH"
}


# === 地址清理 ===
def clean_city_name(name):
    if pd.isna(name):
        return name
    name = str(name).strip()
    name = re.sub(r"　", "", name)
    name = re.sub(r"台", "臺", name)
    return name


# === 把「區域別」拆成 city / district ===
def split_city_district(area):
    if pd.isna(area):
        return None, None

    area = str(area).strip()

    # 正規表達式：縣市 + 區
    match = re.match(r"(.+[縣市])(.+區)", area)
    if match:
        return clean_city_name(match.group(1)), clean_city_name(match.group(2))

    return None, None


# === 主轉換流程 ===
def transform(df):
    print("⚙️ [T] Transform - 開始人口資料清理與整合...")

    print("📌 原始欄位：", df.columns.tolist())

    # === 檢查必備欄位 ===
    required_cols = ["統計年月", "區域別", "村里", "戶數", "人口數", "男", "女"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise Exception(f"❌ Excel 欄位遺失：{missing}")

    # === 拆 city / district ===
    df[["city", "district"]] = df["區域別"].apply(
        lambda x: pd.Series(split_city_district(x))
    )

    # 清理城市名稱
    df["city"] = df["city"].apply(clean_city_name)
    df["district"] = df["district"].apply(clean_city_name)

    # === MYSQL location loc_id 對應 ===
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = os.getenv("MYSQL_PORT")
    db_name = os.getenv("MYSQL_DB_NAME")

    engine = create_engine(
        f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}"
    )
    df_loc = pd.read_sql("SELECT loc_id, city, district FROM location", con=engine)

    def get_loc_id(row):
        city = row["city"]
        district = row["district"]

        # 1. city + district 精準比對
        match = df_loc[(df_loc.city == city) & (df_loc.district == district)]
        if len(match) > 0:
            return match.iloc[0]["loc_id"]

        # 2. 若找不到 → city 單比對
        match = df_loc[df_loc.city == city]
        if len(match) > 0:
            return match.iloc[0]["loc_id"]

        return None

    df["loc_id"] = df.apply(get_loc_id, axis=1)

    # === 只保留六都 ===
    df = df[df["city"].isin(CITY_LOC_MAP.keys())].reset_index(drop=True)

    # === 建立 id ===
    df["id"] = [f"po{str(i+1).zfill(4)}" for i in range(len(df))]

    df["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # === 欄位英文 rename ===
    df.rename(columns={
        "統計年月": "year_month",
        "村里": "village",
        "戶數": "household",
        "人口數": "population",
        "男": "male",
        "女": "female"
    }, inplace=True)

    # === 最終欄位排序 ===
    df = df[[
        "id",
        "loc_id",
        "year_month",
        "city",
        "district",
        "village",
        "household",
        "population",
        "male",
        "female",
        "update_time"
    ]]

    # === 輸出結果 ===
    df.to_csv(PROCESSED_PATH, index=False, encoding="utf-8-sig")
    print(f"📊 已輸出六都人口資料：{PROCESSED_PATH}")
    print(f"✅ 六都資料筆數：{len(df)}")

    return df
