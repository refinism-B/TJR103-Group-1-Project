import os
from mods import extractdata as ed
from mods import readdata as rd
from dotenv import load_dotenv

# 讀取.env檔案
load_dotenv()

raw_path = "data/raw/hospital_data.csv"
processed_path = "data/processed/hospital_data_ETL.csv"


if __name__ == "__main__":
    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 初步ETL
    # 只保留name和address
    need_columns = ["name", "address"]
    df = df[need_columns]

    # 移除:前面的資料
    df["address"] = (
        df["address"]
        .str.split("：")
        .str[-1]
        .str.replace(" ", "", regex=False)
        .str.strip()
    )

    # 執行正則表達比對並建立city, district欄位
    df["city"], df["district"] = zip(*df["address"].apply(ed.extract_city_district))

    # 只取出city非空值的資料，其他drop，所以只會留下六都資訊
    df = df[df["city"].notna()].reset_index(drop=True)

    # ------------------------------------------------------------

    # 執行合併
    host = os.getenv("MYSQL_IP")
    port = int(os.getenv("MYSQL_PORTT"))
    user = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    db = os.getenv("MYSQL_DB_NAME")
    id_sign = "hp"
    API_KEY = os.getenv("GOOGLE_MAP_KEY_CHGWYELLOW")

    df_final = ed.gdata_etl(
        df,
        API_KEY,
        host,
        port,
        user,
        password,
        db,
        id_sign=id_sign,
        save_path=processed_path,
    )
