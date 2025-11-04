from utils import extractdata as ed
from utils import readdata as rd


def main():
    id_sign = "hp"

    raw_path = "airflow/data/processed/hospital/hospital_data_cleaned.csv"
    processed_path = "airflow/data/processed/hospital/hospital_data_id.csv"

    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 與location表合併並產生loc_id
    df = ed.create_id(df, id_sign, processed_path)


if __name__ == "__main__":
    main()
