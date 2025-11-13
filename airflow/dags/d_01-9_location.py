import os
import requests
import pandas as pd
from io import BytesIO
from airflow.decorators import dag, task
from datetime import datetime, date, timedelta
from utils.config import LOCATION_AREA_URL, LOCATION_AREA_COLUMNS, LOC_ID_STR, LOCATION_FINAL_COLUMNS, TAIWAN_CITY_LIST
from tasks import pandas_mod as pdm
from tasks import database_file_mod as dfm


# 設定DAG基本資訊
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="d_01-9_location",
    default_args=default_args,
    description="[每日更新]爬取每日寵物登記數",
    schedule_interval="0 20 5 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "monthly", "location"]
)
def d_01_9_location():
    @task
    def E_get_location_area_data(url: str) -> pd.DataFrame:
        res = requests.get(url=url)
        excel_data = BytesIO(res.content)
        df = pd.read_excel(excel_data)
        print("完成excel轉df")

        df.columns = LOCATION_AREA_COLUMNS
        print("完成欄位重命名")

        df = df.drop(columns=["population", "density"])
        print("完成人口和密度資料去除")

        df["location"] = df["location"].str.replace(
            " ", "").str.replace("　", "").str.replace("※", "")
        drop_idx = df[df["location"] == "總計"].index
        df = df.drop(index=range(0, drop_idx[1]+1), axis=0)
        df = df.reset_index(drop=True)
        print("完成資料清理")

        return df

    @task
    def T_add_city_columns(df: pd.DataFrame, city_list: list) -> pd.DataFrame:
        city_index = []
        for city in city_list:
            idx = df.index[df["location"] == city].tolist()
            city_index.append(int(idx[0]))

        df["city"] = None
        for n in range(0, len(city_index)):
            if n < len(city_index) - 1:
                df.loc[city_index[n]:city_index[n+1]-1, "city"] = city_list[n]
            else:
                df.loc[city_index[n]:, "city"] = city_list[n]

        return df

    @task
    def T_adjust_columns(df: pd.DataFrame) -> pd.DataFrame:
        new_columns = ["district", "area", "city"]
        df.columns = new_columns

        new_order = ["city", "district", "area"]
        df = df[new_order]

        return df

    @task
    def T_round_area_column(df: pd.DataFrame) -> pd.DataFrame:
        df["area"] = df["area"].apply(float)
        df["area"] = df["area"].round(3)

        return df

    @task
    def L_location_raw_save_setting():
        folder = "/opt/airflow/data/raw/location"
        file_name = "location_raw.csv"

        return {"folder": folder, "file_name": file_name}

    ############################################################
    """
    尚未設定人口檔案路徑
    """
    ############################################################
    @task
    def S_get_population_read_setting():
        folder = "/opt/airflow/data/processed/population"
        file_name = "raw_population.csv"

        return {
            "folder": folder,
            "file_name": file_name
        }

    @task
    def T_rename_population_columns(df: pd.DataFrame):
        columns = ["city", "district", "population"]
        df.columns = columns

        df["district"] = df["district"].str.replace("　", "")

        return df

    @task
    def T_merge_df_loc_and_population(df_loc: pd.DataFrame, df_popu: pd.DataFrame) -> pd.DataFrame:
        df_main = df_loc.merge(df_popu, how="left", on=["city", "district"])

        return df_main

    @task
    def T_add_loc_id(df: pd.DataFrame, city_dict: dict) -> pd.DataFrame:
        df = df.dropna(subset="population")

        df["code"] = df["city"].map(city_dict)
        df["seq"] = df.groupby("code").cumcount() + 1
        df["seq"] = df["seq"].apply(lambda x: f"{x:03d}")
        df["loc_id"] = df["code"] + df["seq"]

        df = df.drop(columns=["code", "seq"])

        return df

    @task
    def S_get_location_save_setting():
        folder = "/opt/airflow/data/complete/location"
        file_name = "location.csv"

        return {
            "folder": folder,
            "file_name": file_name
        }
#
#
#
#
#
    # 取得location的序列化資料
    df_loc = E_get_location_area_data(url=LOCATION_AREA_URL)

    # 新增city欄位資料
    df_loc = T_add_city_columns(df=df_loc, city_list=TAIWAN_CITY_LIST)

    # 調整欄位
    df_loc = T_adjust_columns(df=df_loc)

    # 將面積資料取到小數點後三位
    df_loc = T_round_area_column(df=df_loc)

    # 取得raw存檔設定
    raw_save_setting = L_location_raw_save_setting()

    # 先將raw存檔至地端
    dfm.L_save_file_to_csv_by_dict(save_setting=raw_save_setting, df=df_loc)

    # 取得人口檔案讀取資訊
    population_read_setting = S_get_population_read_setting()

    # 讀取人口資料檔案
    df_popu = dfm.E_load_file_from_csv_by_dict(
        read_setting=population_read_setting)

    # 修改人口資料的欄位名
    df_popu = T_rename_population_columns(df=df_popu)

    # 根據市和區欄位Join地區和人口資料
    df_main = T_merge_df_loc_and_population(df_loc=df_loc, df_popu=df_popu)

    # 加上loc_id
    df_main = T_add_loc_id(df=df_main, city_dict=LOC_ID_STR)

    # 調整欄位順序
    df_main = pdm.T_sort_columns(df=df_main, new_cols=LOCATION_FINAL_COLUMNS)

    # 取得最終存檔路徑資訊
    final_save_setting = S_get_location_save_setting()

    # 存檔至地端
    dfm.L_save_file_to_csv_by_dict(df=df_main, save_setting=final_save_setting)

    # 上傳至資料庫
    # col_str = pdm.S_get_columns_str(df=df_main)
    # value_str = pdm.S_get_columns_length_values(df=df_main)

    dfm.L_truncate_and_upload_data_to_db(
        df=df_main, table_name="test_location")


d_01_9_location()
