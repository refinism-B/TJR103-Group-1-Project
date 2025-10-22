from mods import readdata as rd
from mods import savedata as sd
from mods import extractdata as ed

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

# 儲存ETL後的資料
processed_path = "data/processed/pet_hotel_ETL.csv"
sd.store_to_csv_no_index(df_filtered, processed_path)
