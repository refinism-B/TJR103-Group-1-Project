import json
from datetime import date, datetime, timedelta
from pathlib import Path
import time

import pandas as pd
import requests
from airflow.decorators import dag, task
from tasks import database_file_mod as dfm
from tasks import pandas_mod as pdm
from utils import config as cfg
from tasks import GCS_mod as gcs


# 設定DAG基本資訊
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["add412@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="d_test_dynamic_mapping",
    default_args=default_args,
    description="測試task動態映射功能",
    schedule_interval="0 20 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["test"]
)
def d_test_dynamic_mapping():
    @task
    def S_get_requests_data_dict(city_index: int) -> dict:
        """設定request訪問時需要的資料"""
        city_list = list(cfg.CITY_NAME_CODE_DICT.keys())
        city_code = city_list[city_index]
        city_name = cfg.CITY_NAME_CODE_DICT[city_code]

        data_dict = {
            "url": "https://www.pet.gov.tw/Handler/PostData.ashx",
            "headers": {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
            },
            "city": city_code,
            "city_name": city_name,
            "animal": {"0": "犬", "1": "貓"},
            "today": date.today(),
            "start": date.today() - timedelta(days=1),
            "start_date": (date.today() - timedelta(days=1)).strftime("%Y/%m/%d"),
            "end_date": date.today().strftime("%Y/%m/%d"),
            "folder": "/opt/airflow/data/raw/registrue",
            "file_name": f"{city_name}.csv"
        }
        return data_dict

    @task
    def S_create_post_data(dict_name: dict, ani: str) -> dict:
        """建立post訪問時需要帶入的資料"""
        start_date = dict_name["start_date"]
        end_date = dict_name["end_date"]
        city = dict_name["city"]

        return {
            "Method": "O302C_2",
            "Param": json.dumps(
                {
                    "SDATE": start_date,
                    "EDATE": end_date,
                    "Animal": ani,
                    "CountyID": city,
                }),
        }

    def post_requests(url: str, headers: dict, data: dict) -> json:
        """進行requests訪問並回傳取得的json檔案"""
        res = requests.post(url=url, headers=headers, data=data)
        res.raise_for_status()
        res.encoding = "utf-8-sig"

        # 因資料是以json格式儲存和回傳，故需json解碼
        data_orig = json.loads(res.text)
        data_str = data_orig.get("Message", "[]")
        data_json = json.loads(data_str)

        return data_json

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def E_get_main_data(data_pairs: tuple) -> pd.DataFrame:
        """將取得的json檔案轉換為dataframe"""

        data_dict = data_pairs[0]
        data = data_pairs[1]

        data_json = post_requests(
            url=data_dict["url"], headers=data_dict["headers"], data=data)
        df = pd.DataFrame(data_json)
        time.sleep(5)

        return df

    @task
    def T_add_columns(data_pairs: tuple, ani: str) -> pd.DataFrame:
        """將dataframe加上日期、寵物類別、城市和更新日期欄位"""
        df = data_pairs[0]
        data_dict = data_pairs[1]

        df["date"] = data_dict["start_date"]
        df["animal"] = ani
        df["city"] = data_dict["city"]
        df["update_date"] = data_dict["end_date"]

        return df

    @task
    def S_get_save_setting(dict_name: dict) -> dict:
        """取得存檔資訊"""
        folder = dict_name["folder"]
        file_name = dict_name["file_name"]

        return {"folder": folder, "file_name": file_name}

    @task
    def T_trans_city_to_ch(df: pd.DataFrame, city_dict: dict) -> pd.DataFrame:
        """將城市從代碼轉換成對應的中文"""
        df["city"] = df["city"].apply(lambda x: city_dict[x])

        return df

    @task
    def T_clean_district_value(df: pd.DataFrame) -> pd.DataFrame:
        """將區的郵遞區號去除"""
        df["district"] = df["district"].apply(lambda x: x[3:])

        return df

    @task
    def T_df_merge_location(df_main: pd.DataFrame, df_loc: list[dict]) -> pd.DataFrame:
        """與location表join取得loc id"""
        df_loc = pd.DataFrame(df_loc)
        df_loc = df_loc[["loc_id", "city", "district"]]

        df_main = df_main.merge(df_loc, how="left", on=["city", "district"])
        df_main.drop(columns=["city", "district"], axis=1, inplace=True)

        return df_main

    @task
    def L_complete_save_file(df: pd.DataFrame):
        """將完成爬取的檔案儲存至地端"""
        file_date = date.today().strftime("%Y-%m-%d")
        folder = Path(f"/opt/airflow/data/complete/registrue/dt={file_date}")
        folder.mkdir(parents=True, exist_ok=True)

        file_name = "registration.csv"
        path = folder / file_name
        try:
            df.to_csv(path, index=False, encoding="utf-8-sig")
            print(f"{file_date}資料地端存檔成功！")

        except Exception as e:
            print(f"{file_date}資料存檔失敗：{e}")

    @task
    def S_get_gcs_setting():
        """取得上傳至GCS所需的路徑資訊"""
        file_date = date.today().strftime("%Y-%m-%d")
        bucket_name = "tjr103-1-project-bucket"
        destination = f"data/complete/registration/dt={file_date}/registration.csv"
        source_file_name = f"/opt/airflow/data/complete/registrue/dt={file_date}/registration.csv"

        return {
            "bucket_name": bucket_name,
            "destination": destination,
            "source_file_name": source_file_name
        }

    # 取得六都與代碼對照表
    city_dict = cfg.CITY_NAME_CODE_DICT
    index_list = [0, 1, 2, 3, 4, 5]
    animal_list = ["0", "1"]

    # 建立六都的爬蟲資料表
    data_dict = S_get_requests_data_dict.expand(city_index=index_list)

    # 建立六都貓、狗的post資料表
    data_loc_ani = S_create_post_data.expand(
        dict_name=data_dict, ani=animal_list)

    data_pairs = pdm.T_zip_two_data(list1=data_dict, list2=data_loc_ani)

    # 逐一爬取六都貓狗資料
    df_loc_ani = E_get_main_data.expand(data_pairs=data_pairs)

    data_pairs_2 = pdm.T_zip_two_data(list1=df_loc_ani, list2=data_dict)

    # 將表加上動物及一些紀錄欄位
    df_loc_ani_col = T_add_columns.expand(
        data_pairs=data_pairs_2, ani=animal_list)

    # 將各都的貓狗df合併
    df_loc = pdm.T_dynamic_combine_df(df_list=df_loc_ani_col)

    # 取得六都存檔設定
    raw_save_setting = S_get_save_setting.expand(dict_name=data_dict)

    # 將六都df合併為單一主表
    df_main = pdm.T_combine_df_list(df_list=df_loc)

    # 重新命名欄位為可讀名稱
    df_main = pdm.T_rename_columns(
        df=df_main, col_list=cfg.PET_REGIS_COLUMNS_NAME)

    # 將city欄位中的值轉換成中文
    df_main = T_trans_city_to_ch(df=df_main, city_dict=city_dict)

    # 移除不要的欄位
    df_main = pdm.T_drop_columns(
        df=df_main, drop_list=cfg.PET_REGIS_DROP_COLUMNS)

    # 將區的郵遞區號去除
    df_main = T_clean_district_value(df=df_main)

    pdm.L_print_df(df=df_main)


# d_test_dynamic_mapping()
