from mods import extractdata as ed
from mods import readdata as rd
from mods import savedata as sd
from dotenv import load_dotenv


# 讀取.env檔案
load_dotenv()

raw_path = "data/raw/hospital_data.csv"
processed_path = "data/processed/hospital_data_c_d.csv"

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
    df = ed.extract_city_district_from_df(df, "address")

    # 儲存資料
    sd.store_to_csv_no_index(df, processed_path)
