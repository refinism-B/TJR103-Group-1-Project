import os
from utils import extractdata as ed
from utils import readdata as rd
from dotenv import load_dotenv


def main():
    # 讀取.env檔案
    load_dotenv()

    API_KEY = os.getenv("GOOGLE_MAP_KEY_CHGWYELLOW")

    raw_path = "/opt/airflow/data/processed/hospital/hospital_data_place_id.csv"
    processed_path = "/opt/airflow/data/processed/hospital/hospital_data_detail.csv"

    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 透過place_id查找詳細資料
    df = ed.gdata_info(df, API_KEY, processed_path)


if __name__ == "__main__":
    main()
