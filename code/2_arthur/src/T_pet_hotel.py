from mods import readdata as rd
from mods import extractdata as ed
from mods import date_mod as dm
import os
from dotenv import load_dotenv

raw_path = "data/raw/pet_establishment.csv"
google_path = "data/processed/pet_hotel_GOOGLE.csv"
final_path = "data/processed/pet_hotel_ETL.csv"

host = os.getenv("MYSQL_IP")
port = int(os.getenv("MYSQL_PORTT"))
user = os.getenv("MYSQL_USERNAME")
password = os.getenv("MYSQL_PASSWORD")
db = os.getenv("MYSQL_DB_NAME")
id_sign = "ht"
API_KEY = os.getenv("GOOGLE_MAP_KEY_CHGWYELLOW")

if __name__ == "__main__":

    load_dotenv()

    df = rd.get_csv_data(raw_path)

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
    df_filtered = ed.extract_city_district_from_df(df_filtered, "legaladdress")

    # 只留下名稱、地址、市和區
    df_filtered = df_filtered[["legalname", "legaladdress", "city", "district"]]

    # 修改欄位名稱
    df_filtered = df_filtered.rename(
        columns={
            "legalname": "name",
            "legaladdress": "address",
        }
    )

    # 執行google資料驗證
    df_google = ed.gdata_etl(df_filtered, API_KEY, google_path)

    # ------------------------------------------------------------
    # 修改opening_hours欄位
    # ------------------------------------------------------------
    df_google["opening_hours"] = df_google["opening_hours"].apply(
        dm.trans_op_time_to_hours
    )

    # ------------------------------------------------------------
    # 轉換pd空值
    # ------------------------------------------------------------
    for col in df_google.columns:
        df_google[col] = df_google[col].apply(ed.to_sql_null)

    # 與location表合併
    df_merged = ed.merge_loc(df_google, host, port, user, password, db)

    # 產生id號碼
    df_id = ed.create_id(df_merged, id_sign)

    # 整理欄位成最終可以寫入DB的型態並儲存
    df_final = ed.to_sql_data(df_id, final_path)
