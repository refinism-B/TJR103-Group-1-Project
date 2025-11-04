import os
from mods import extractdata as ed
from mods import readdata as rd
from dotenv import load_dotenv


# 讀取.env檔案
load_dotenv()

host = os.getenv("MYSQL_IP")
port = int(os.getenv("MYSQL_PORTT"))
user = os.getenv("MYSQL_USERNAME")
password = os.getenv("MYSQL_PASSWORD")
db = os.getenv("MYSQL_DB_NAME")

raw_path = "data/processed/hospital/hospital_data_id.csv"
processed_path = "data/processed/hospital/hospital_data_merged.csv"

if __name__ == "__main__":
    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 與location表合併並產生loc_id
    df = ed.merge_loc(df, host, port, user, password, db, processed_path)
