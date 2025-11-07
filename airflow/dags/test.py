import os
import time
from datetime import date, datetime, timedelta

import geopandas as gpd
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import Point
from sqlalchemy import create_engine
from pathlib import Path
from airflow.decorators import dag, task

from utils.config import GSEARCH_CITY_CODE, STORE_TYPE_ENG_CH_DICT, STORE_TYPE_CODE_DICT
from utils import gmap_mod as gm
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
    dag_id="d_test04",
    default_args=default_args,
    description="[每月更新]透過經緯度爬取六都「寵物美容」列表",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "monthly", "salon"]
)
def d_test():
    def S_get_keyword_dict(dict_name: dict, index: int) -> dict:
        keyword_ch_list = list(dict_name.keys())
        keyword_list = list(dict_name.values())

        return {"keyword": keyword_ch_list[index], "file_name": keyword_list[index]}

    def S_get_read_setting(keyword_dict: dict) -> dict:
        folder = f"/opt/airflow/data/processed/{keyword_dict['file_name']}"
        file_name = f"{keyword_dict['file_name']}_place_id.csv"

        return {"folder": folder, "file_name": file_name}

    @task
    def S_get_id_columns_setting(df: pd.DataFrame, type_dict: dict, keyword_dict: dict) -> pd.DataFrame:
        store_type_name = keyword_dict["keyword"]
        id_str = type_dict[store_type_name]

        return {"id_cols": "id", "id_str": id_str}

    @task
    def T_add_id_columns(df: pd.DataFrame):
        df["id"] = ""

        return df

    # 先定義要執行的商店類別
    # 0為寵物美容、1為寵物餐廳、2為寵物用品
    keyword_dict = S_get_keyword_dict(
        dict_name=STORE_TYPE_ENG_CH_DICT, index=0)

    # 設定讀取路徑及檔案名
    read_setting = S_get_read_setting(keyword_dict=keyword_dict)

    # 將place id表讀入
    df_place = dfm.E_load_file_from_csv_by_dict(read_setting=read_setting)

    id_setting = S_get_id_columns_setting(
        df=df_place, type_dict=STORE_TYPE_CODE_DICT, keyword_dict=keyword_dict)

    df_place = T_add_id_columns(df=df_place)

    df_place = pdm.T_reassign_id(df=df_place, setting_dict=id_setting)


d_test()
