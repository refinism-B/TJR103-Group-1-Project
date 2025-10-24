from mods import readdata as rd
from mods import savedata as sd
from mods import extractdata as ed
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
    (df_filtered["busitem"].str.contains("C")) & (df_filtered["state_flag"] == "N")
]

# 找出屬於六都的機構
df_filtered["city"], df_filtered["district"] = zip(
    *df_filtered["legaladdress"].apply(ed.extract_city_district)
)
df_filtered = df_filtered[df_filtered["city"].notna()].reset_index(drop=True)

# 搜尋名字有旅、住、宿的資料
df_filtered = df_filtered[df_filtered["legalname"].str.contains("旅|住|宿", regex=True)]

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

# 透過place_id找到詳細資料
result = []
for _, row in df_filtered.iterrows():
    result.append(gm.gmap_info(row["name"], API_KEY, row["place_id"]))
df_checked = pd.DataFrame(result)

df_merged = df_filtered.merge(
    df_checked,
    how="outer",
    left_on=df_filtered["place_id"],
    right_on=df_checked["place_id"],
    suffixes=["_filtered", "_checked"],
)

# 去除重複欄位
df_merged = df_merged.drop(columns=["place_id_filtered", "place_id_checked"])

if not df_merged.isna().any():
    # 儲存ETL後的資料
    processed_path = "data/processed/pet_hotel_ETL.csv"
    sd.store_to_csv_no_index(df_filtered, processed_path)
else:
    print(Fore.RED + "[✗] DataFrame內有空值，請檢查!")
