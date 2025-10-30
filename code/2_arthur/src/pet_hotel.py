from mods import readdata as rd
from mods import savedata as sd
from mods import extractdata as ed
import os
from dotenv import load_dotenv

if __name__ == "__main__":

    load_dotenv()

    url = (
        "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=fNT9RMo8PQRO"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
    }

    # 讀取API檔案
    df = rd.get_json_data_no_verify(url)

    # 儲存raw data
    raw_path = "data/raw/pet_establishment.csv"
    sd.store_to_csv_no_index(df=df, path=raw_path)

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

    # 執行合併ETL
    host = os.getenv("MYSQL_IP")
    port = int(os.getenv("MYSQL_PORTT"))
    user = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    db = os.getenv("MYSQL_DB_NAME")
    id_sign = "ht"
    API_KEY = os.getenv("GOOGLE_MAP_KEY_CHGWYELLOW")
    processed_path = "data/processed/pet_hotel_ETL.csv"

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
