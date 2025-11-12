import os
import requests
import pandas as pd
from io import BytesIO
from airflow.decorators import dag, task
from datetime import datetime, date, timedelta
from utils.config import LOCATION_AREA_URL, LOCATION_AREA_COLUMNS
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

        return df

    @task
    def S_get_city_list(df: pd.DataFrame) -> list:
        df_city_list = df.copy()
        df_city_list = df_city_list.iloc[3:29]

        df_city_list["location"] = df_city_list["location"].str.replace(
            " ", "").str.replace("　", "").str.replace("※", "")
        city_list = list(df_city_list["location"])

        return city_list

    @task
    def T_drop_unnecessary_columns(df: pd.DataFrame) -> pd.DataFrame:
        df["location"] = df["location"].str.replace(
            " ", "").str.replace("　", "").str.replace("※", "")
        drop_idx = df[df["location"] == "總計"].index
        df = df.drop(index=range(0, drop_idx[1]+1), axis=0)
        df = df.reset_index(drop=True)

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
        new_columns = ["district", "population", "area", "density", "city"]
        df.columns = new_columns

        df = df.drop(columns=["population", "density"])

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
#
#
#
#
#
    # 取得location原檔
    df_loc = E_get_location_area_data(url=LOCATION_AREA_URL)

    # 重新命名欄位
    df_loc = pdm.T_rename_columns(df=df_loc, col_list=LOCATION_AREA_COLUMNS)

    # 取得city_list
    city_list = S_get_city_list(df=df_loc)

    # 將不必要的雜亂的欄位去除
    df_loc = T_drop_unnecessary_columns(df=df_loc)

    # 新增city欄位資料
    df_loc = T_add_city_columns(df=df_loc, city_list=city_list)

    # 調整欄位
    df_loc = T_adjust_columns(df=df_loc)

    # 將面積資料取到小數點後三位
    df_loc = T_round_area_column(df=df_loc)

    # 取得raw存檔設定
    raw_save_setting = L_location_raw_save_setting()

    # 先將raw存檔至地端
    dfm.L_save_file_to_csv_by_dict(save_setting=raw_save_setting, df=df_loc)


d_01_9_location()
