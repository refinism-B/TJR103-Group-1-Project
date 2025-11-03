from mods import extractdata as ed
from mods import readdata as rd


id_sign = "hp"

raw_path = "data/processed/hospital_data_cleaned.csv"
processed_path = "data/processed/hospital_data_id.csv"

if __name__ == "__main__":
    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 與location表合併並產生loc_id
    df = ed.create_id(df, id_sign, processed_path)
