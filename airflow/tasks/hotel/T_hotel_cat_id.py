from utils import extractdata as ed
from utils import readdata as rd
from dotenv import load_dotenv
import os


def main():
    # 載入.env檔案
    load_dotenv()

    raw_path = "/opt/airflow/data/processed/hotel/hotel_data_merged.csv"
    processed_path = "/opt/airflow/data/processed/hotel/hotel_data_cat_id.csv"

    host = os.getenv("MYSQL_IP")
    port = int(os.getenv("MYSQL_PORTT"))
    user = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    db = os.getenv("MYSQL_DB_NAME")

    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 與category表合併並產生cat_id
    df = ed.cat_id(df, host, port, user, password, db, processed_path, "hotel")


if __name__ == "__main__":
    main()
