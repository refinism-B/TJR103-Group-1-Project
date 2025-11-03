import os
from mods import extractdata as ed
from mods import readdata as rd
from dotenv import load_dotenv


# 讀取.env檔案
load_dotenv()

API_KEY = os.getenv("GOOGLE_MAP_KEY_CHGWYELLOW")

raw_path = "data/processed/hospital_data_c_d.csv"
processed_path = "data/processed/hospital_data_placd_id.csv"

if __name__ == "__main__":
    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 透過google api找到place_id
    df_place_id = ed.gdata_place_id(df, API_KEY, processed_path)
