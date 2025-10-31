from mods import extractdata as ed
from mods import readdata as rd


raw_path = "data/processed/hotel_data_cat_id.csv"
processed_path = "data/processed/hotel_data_final.csv"

if __name__ == "__main__":
    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 與location表合併並產生loc_id
    df = ed.to_sql_data(df, processed_path)
