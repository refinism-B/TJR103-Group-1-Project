from mods import readdata as rd
from mods import extractdata as ed
import os
from dotenv import load_dotenv

raw_path = "data/raw/pet_establishment.csv"
processed_path = "data/processed/pet_hotel_ETL.csv"

if __name__ == "__main__":

    load_dotenv()

    df = rd.get_csv_data(raw_path)

    # 執行初步ETL
    # 去除不必要的欄位
    df_filtered = df[
        ["legalname", "legaladdress", "busitem", "animaltype", "state_flag"]
    ]

    # busitem = A表示繁殖，C表示寄養，state_flag = N表示營業中
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

    # ------------------------------------------------------------

    # 執行合併
    host = os.getenv("MYSQL_IP")
    port = int(os.getenv("MYSQL_PORTT"))
    user = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    db = os.getenv("MYSQL_DB_NAME")
    id_sign = "ht"
    API_KEY = os.getenv("GOOGLE_MAP_KEY_CHGWYELLOW")

    df_final = ed.gdata_etl(
        df_filtered,
        API_KEY,
        host,
        port,
        user,
        password,
        db,
        id_sign=id_sign,
        save_path=processed_path,
    )
