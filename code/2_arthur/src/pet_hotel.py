from mods import readdata as rd
from mods import savedata as sd
from mods import extractdata as ed
from mods import connectDB as connDB
import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from mods import gmap as gm
from colorama import Fore

load_dotenv()

url = "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=fNT9RMo8PQRO"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
}

host = os.getenv("MYSQL_IP")
port = int(os.getenv("MYSQL_PORTT"))
user = os.getenv("MYSQL_USERNAME")
password = os.getenv("MYSQL_PASSWORD")
db = os.getenv("MYSQL_DB_NAME")

# 讀取API檔案
df = rd.get_json_data_no_verify(url)

# 儲存raw data
raw_path = "data/raw/pet_establishment.csv"
sd.store_to_csv_no_index(df=df, path=raw_path)

# 執行ETL
# 去除不必要的欄位
df_filtered = df[["legalname", "legaladdress", "busitem", "animaltype", "state_flag"]]

# busitem = C表示寄養，state_flag = N表示營業中
df_filtered = df_filtered[
    (~df_filtered["busitem"].str.contains("A", na=False))
    & (df_filtered["busitem"].str.contains("C", na=False))
    & (df_filtered["state_flag"] == "N")
]

# 找出屬於六都的機構
df_filtered["city"], df_filtered["district"] = zip(
    *df_filtered["legaladdress"].apply(ed.extract_city_district)
)
df_filtered = df_filtered[df_filtered["city"].notna()].reset_index(drop=True)

# 只留下名稱、地址、市和區
df_filtered = df_filtered[["legalname", "legaladdress", "city", "district"]]

# 修改欄位名稱
df_filtered = df_filtered.rename(
    columns={
        "legalname": "name",
        "legaladdress": "address",
    }
)

# 透過google api並傳送名稱與地址取得place_id
API_KEY = os.getenv("GOOGLE_MAP_KEY_CHGWYELLOW")
result = []

# enumerate會自動將被iterate的物件附上index
for i, (idx, row) in enumerate(df_filtered.iterrows()):
    query = f"{row['name']} {row['address']}"

    result.append(gm.get_place_id(API_KEY, query))
df_filtered["place_id"] = np.nan
df_filtered.loc[:, "place_id"] = result

# drop place_id為空的資料
df_filtered = df_filtered.dropna(subset="place_id")

# 透過place_id找到詳細資料
result = []
for _, row in df_filtered.iterrows():
    result.append(gm.gmap_info(row["name"], API_KEY, row["place_id"]))
df_checked = pd.DataFrame(result)

df_checked = df_checked.dropna(subset="place_id")

df_merged = df_filtered.merge(
    df_checked,
    how="outer",
    left_on=df_filtered["place_id"],
    right_on=df_checked["place_id"],
    suffixes=["_filtered", "_checked"],
)

# 只留下business_status為OPERATIONAL的資料
df_merged = df_merged[df_merged["business_status"] == "OPERATIONAL"]

# 去除重複欄位
df_merged = df_merged.drop(columns=["place_id_filtered", "place_id_checked"])

# 修改columns順序
revised_columns = [
    "key_0",
    "name_filtered",
    "name_checked",
    "address_filtered",
    "address_checked",
    "phone",
    "city",
    "district",
    "business_status",
    "opening_hours",
    "rating",
    "rating_total",
    "longitude",
    "latitude",
    "map_url",
    "newest_review",
]
df_merged = df_merged[revised_columns]

df_merged = df_merged.drop_duplicates(subset="key_0")

# 需要計算的欄位空值補0
fillna_columns = ["opening_hours", "rating", "rating_total", "newest_review"]
df_merged[fillna_columns] = df_merged[fillna_columns].fillna(0)

# 連線DB
conn, cursor = connDB.connect_db(host, port, user, password, db)

# 讀取location表格的資料並轉成DataFrame
df_loc = connDB.get_loc_table(conn, cursor)

# merge df_merged和df_loc
df_final = df_merged.merge(df_loc, left_on="district", right_on="district", how="left")
df_final = df_final.drop(columns=["city_y"])
df_final = df_final.rename(columns={"city_x": "city"})

# 重新編排columns順序
columns = [
    "key_0",
    "name_checked",
    "address_checked",
    "phone",
    "city",
    "district",
    "loc_id",
    "business_status",
    "opening_hours",
    "rating",
    "rating_total",
    "longitude",
    "latitude",
    "map_url",
    "newest_review",
]
df_final = df_final[columns]

if not df_merged.isna().any():
    # 儲存ETL後的資料
    processed_path = "data/processed/pet_hotel_ETL.csv"
    sd.store_to_csv_no_index(df_merged, processed_path)
else:
    print(Fore.RED + "[✗] DataFrame內有空值，請檢查!")
