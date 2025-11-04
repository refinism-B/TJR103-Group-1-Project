from utils import extractdata as ed
from utils import readdata as rd
from dotenv import load_dotenv


def main():
    # 讀取.env檔案
    load_dotenv()

    raw_path = "airflow/data/processed/hospital/hospital_data_detail.csv"
    processed_path = "airflow/data/processed/hospital/hospital_data_cleaned.csv"

    # 讀取原始檔案
    df = rd.get_csv_data(raw_path)

    # 欄位清洗與補空值
    df = ed.clean_sort(df, processed_path)


if __name__ == "__main__":
    main()
