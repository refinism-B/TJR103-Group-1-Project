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
from tasks.pipeline import gmap_full_search as gfs


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
    dag_id="d_test05",
    default_args=default_args,
    description="[每月更新]透過經緯度爬取六都「寵物美容」列表",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "monthly", "salon"]
)
def d_test():
    STORE_TYPE_ENG_CH_DICT = {
        "寵物美容": "salon",
        "寵物餐廳": "restaurant",
        "寵物用品": "supplies"
    }

    @task
    def S_get_keyword_dict(dict_name: dict, index: int):
        keyword_list = list(dict_name.keys())
        file_name_list = list(dict_name.values())
        return {
            "keyword": keyword_list[index], "file_name": file_name_list[index]
        }

    def S_get_main_save_setting(keyword_dict: dict) -> dict:
        folder = f"/opt/airflow/data/processed/{keyword_dict['file_name']}"
        file_name = f"{keyword_dict['file_name']}_place_id.csv"

        return {"folder": folder, "file_name": file_name}

    @task
    def L_save_file_to_csv_by_dict(save_setting: dict, df: pd.DataFrame):
        try:
            folder = Path(save_setting["folder"])
            folder.mkdir(parents=True, exist_ok=True)
            file_name = save_setting["file_name"]
            path = folder / file_name
            df.to_csv(path, index=False, encoding="utf-8-sig")

            print(f"{file_name}地端存檔成功！")
        except Exception as e:
            print({f"{file_name}地端存檔失敗：{e}"})

    # 0為寵物美容，1為寵物餐廳，2為寵物用品
    keyword_dict = gfs.S_get_keyword_dict(
        dict_name=STORE_TYPE_ENG_CH_DICT, index=1)

    # 取得存檔設定
    main_save_setting = gfs.S_get_main_save_setting(keyword_dict=keyword_dict)

    df_main = pd.DataFrame({
        "name": ["a", "b", "c"],
        "age": [10, 20, 30]
    })

    # 存檔至地端
    dfm.L_save_file_to_csv_by_dict(save_setting=main_save_setting, df=df_main)


d_test()
