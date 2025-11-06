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

from utils.config import GSEARCH_CITY_CODE, STORE_TYPE_ENG_CH_DICT
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
    dag_id="d_02-2_test",
    default_args=default_args,
    description="[每月更新]透過經緯度爬取六都「寵物美容」列表",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["bevis", "monthly", "salon"]  # Optional: Add tags for better filtering in the UI
)



def d_02_test():
    @task
    def S_get_search_data_dict(dict_name: dict) -> dict:
        return {
            "today":date.today().strftime("%Y/%m/%d"),
            "city_list":list(dict_name.keys()),
            "city_eng_list":list(dict_name.values()),
            "city_dict":dict_name
        }


    @task
    def S_get_city_data(dict_name: dict, index: int) -> dict:
        return {
            "city_name":dict_name["city_list"][index],
            "city_code":dict_name["city_eng_list"][index]
        }


    @task
    def S_get_keyword_dict(dict_name: dict, index: int):
        keyword_list = list(dict_name.keys())
        file_name_list = list(dict_name.values())
        return {
            "keyword":keyword_list[index], "file_name":file_name_list[index]
        }

    @task
    def S_get_save_setting(keyword_dict: dict, city_dict: dict):
        folder = f"/opt/airflow/data/raw/{keyword_dict['file_name']}"
        file_name = f"{city_dict['city_code']}_{keyword_dict['file_name']}.csv"
        return {"folder":folder, "file_name":file_name}
    
    @task
    def print_result(save_setting: dict):

        print(save_setting["folder"])
        print(save_setting["file_name"])

    @task
    def save_to_file(save_setting: dict, df: pd.DataFrame):
        folder = Path(save_setting["folder"])
        file_name = save_setting["file_name"]
        path = folder / file_name
        df.to_csv(path, index=False, encoding="utf-8")
        print("完成存檔")


    gsearch_dict = S_get_search_data_dict(dict_name=GSEARCH_CITY_CODE)

    TPE_city_dict = S_get_city_data(dict_name=gsearch_dict, index=0)

    keyword_dict = S_get_keyword_dict(dict_name=STORE_TYPE_ENG_CH_DICT, index=0)

    TPE_save_setting = S_get_save_setting(keyword_dict=keyword_dict, city_dict=TPE_city_dict)

    df_TPE = pd.DataFrame({
        "id":["a", "b", "c"],
        "age":[10, 20, 30]
    })

    save_to_file(save_setting=TPE_save_setting, df=df_TPE)


d_02_test()