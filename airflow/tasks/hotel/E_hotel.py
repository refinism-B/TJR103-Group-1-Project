from utils import readdata as rd
from utils import savedata as sd


def main():

    raw_path = "airflow/data/raw/hotel/pet_establishment.csv"

    url = (
        "https://data.moa.gov.tw/Service/OpenData/TransService.aspx?UnitId=fNT9RMo8PQRO"
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
    }

    # 讀取API檔案
    df = rd.get_json_data_no_verify(url)

    # 儲存raw data
    sd.store_to_csv_no_index(df=df, path=raw_path)


if __name__ == "__main__":
    main()
