from mods import extractdata as ed
from mods import readdata as rd
from dotenv import load_dotenv


# 讀取.env檔案
load_dotenv()

raw_path = "data/processed/hotel/hotel_data_detail.csv"
processed_path = "data/processed/hotel/hotel_data_cleaned.csv"

if __name__ == "__main__":
    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 欄位清洗與補空值
    df = ed.clean_sort(df, processed_path)
