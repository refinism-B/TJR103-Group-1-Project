from utils import extractdata as ed
from utils import readdata as rd


def main():
    raw_path = "/opt/airflow/data/processed/hotel/hotel_data_cat_id.csv"
    processed_path = "/opt/airflow/data/data/complete/store/type=hotal/store.csv"

    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 與location表合併並產生loc_id
    df = ed.to_sql_data(df, processed_path)


if __name__ == "__main__":
    main()
