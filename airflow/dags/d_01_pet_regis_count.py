import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
import pandas as pd
import requests
from utils.config import CITY_NAME_CODE_DICT
from tasks import pandas_mod as pdm
from tasks.database_file_mod import L_save_file_to_csv
from airflow.decorators import dag, task


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
    dag_id="d_01_pet_regis_count",
    default_args=default_args,
    description="爬取每日寵物登記數",
    schedule_interval="*/45 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["bevis"]  # Optional: Add tags for better filtering in the UI
)
def d_01_pet_regis_count():
    @task
    def E_get_requests_data_dict(city_dict: dict, city_index: int) -> dict:
        city_list = list(city_dict.keys())
        city_code = city_list[city_index]
        city_name = city_dict[city_code]

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
            "folder": r"/opt/airflow/data/raw/pet_registry",
            "file_name": f"{city_name}.csv"
        }
        return data_dict

    @task
    def E_create_post_data(start_date, end_date, ani, city) -> dict:
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

    def post_requests(url, headers, data) -> json:
        res = requests.post(url=url, headers=headers, data=data)
        res.encoding = "utf-8-sig"

        # 因資料是以json格式儲存和回傳，故需json解碼
        data_orig = json.loads(res.text)
        data_str = data_orig.get("Message", "[]")
        data_json = json.loads(data_str)

        return data_json

    @task
    def E_get_main_data(data_dict, data) -> pd.DataFrame:
        max_tries = 3
        for tries in range(1, max_tries+1):
            try:
                data_json = post_requests(
                    url=data_dict["url"], headers=data_dict["headers"], data=data)
                df = pd.DataFrame(data_json)
                break
            except Exception as e:
                if tries >= max_tries:
                    raise Exception(f"已達最大嘗試次數，仍發生錯誤:{e}，終止程式。")
                else:
                    print(f"發生錯誤：{e}，{tries*4}秒後重試...")
                    time.sleep(tries*4)

        return df

    @task
    def T_add_columns(df: pd.DataFrame, data_dict: dict, ani: str) -> pd.DataFrame:
        df["date"] = data_dict["start_date"]
        df["animal"] = ani
        df["city"] = data_dict["city"]
        df["update_date"] = data_dict["end_date"]

        return df

    city_dict = CITY_NAME_CODE_DICT

    data_dict_NTP = E_get_requests_data_dict(city_dict=city_dict, city_index=0)
    data_dict_TPE = E_get_requests_data_dict(city_dict=city_dict, city_index=1)
    data_dict_TYN = E_get_requests_data_dict(city_dict=city_dict, city_index=2)
    data_dict_TCH = E_get_requests_data_dict(city_dict=city_dict, city_index=3)
    data_dict_TNA = E_get_requests_data_dict(city_dict=city_dict, city_index=4)
    data_dict_KSH = E_get_requests_data_dict(city_dict=city_dict, city_index=5)

    data_NTP_dog = E_create_post_data(
        start_date=data_dict_NTP["start_date"], end_date=data_dict_NTP["end_date"], ani="0", city=data_dict_NTP["city"])
    data_NTP_cat = E_create_post_data(
        start_date=data_dict_NTP["start_date"], end_date=data_dict_NTP["end_date"], ani="1", city=data_dict_NTP["city"])
    data_TPE_dog = E_create_post_data(
        start_date=data_dict_TPE["start_date"], end_date=data_dict_TPE["end_date"], ani="0", city=data_dict_TPE["city"])
    data_TPE_cat = E_create_post_data(
        start_date=data_dict_TPE["start_date"], end_date=data_dict_TPE["end_date"], ani="1", city=data_dict_TPE["city"])
    data_TYN_dog = E_create_post_data(
        start_date=data_dict_TYN["start_date"], end_date=data_dict_TYN["end_date"], ani="0", city=data_dict_TYN["city"])
    data_TYN_cat = E_create_post_data(
        start_date=data_dict_TYN["start_date"], end_date=data_dict_TYN["end_date"], ani="1", city=data_dict_TYN["city"])
    data_TCH_dog = E_create_post_data(
        start_date=data_dict_TCH["start_date"], end_date=data_dict_TCH["end_date"], ani="0", city=data_dict_TCH["city"])
    data_TCH_cat = E_create_post_data(
        start_date=data_dict_TCH["start_date"], end_date=data_dict_TCH["end_date"], ani="1", city=data_dict_TCH["city"])
    data_TNA_dog = E_create_post_data(
        start_date=data_dict_TNA["start_date"], end_date=data_dict_TNA["end_date"], ani="0", city=data_dict_TNA["city"])
    data_TNA_cat = E_create_post_data(
        start_date=data_dict_TNA["start_date"], end_date=data_dict_TNA["end_date"], ani="1", city=data_dict_TNA["city"])
    data_KSH_dog = E_create_post_data(
        start_date=data_dict_KSH["start_date"], end_date=data_dict_KSH["end_date"], ani="0", city=data_dict_KSH["city"])
    data_KSH_cat = E_create_post_data(
        start_date=data_dict_KSH["start_date"], end_date=data_dict_KSH["end_date"], ani="1", city=data_dict_KSH["city"])

    df_NTP_dog = E_get_main_data(data_dict=data_dict_NTP, data=data_NTP_dog)
    df_NTP_cat = E_get_main_data(data_dict=data_dict_NTP, data=data_NTP_cat)
    df_TPE_dog = E_get_main_data(data_dict=data_dict_TPE, data=data_TPE_dog)
    df_TPE_cat = E_get_main_data(data_dict=data_dict_TPE, data=data_TPE_cat)
    df_TYN_dog = E_get_main_data(data_dict=data_dict_TYN, data=data_TYN_dog)
    df_TYN_cat = E_get_main_data(data_dict=data_dict_TYN, data=data_TYN_cat)
    df_TCH_dog = E_get_main_data(data_dict=data_dict_TCH, data=data_TCH_dog)
    df_TCH_cat = E_get_main_data(data_dict=data_dict_TCH, data=data_TCH_cat)
    df_TNA_dog = E_get_main_data(data_dict=data_dict_TNA, data=data_TNA_dog)
    df_TNA_cat = E_get_main_data(data_dict=data_dict_TNA, data=data_TNA_cat)
    df_KSH_dog = E_get_main_data(data_dict=data_dict_KSH, data=data_KSH_dog)
    df_KSH_cat = E_get_main_data(data_dict=data_dict_KSH, data=data_KSH_cat)

    df_NTP_dog = T_add_columns(df=df_NTP_dog, data_dict=data_dict_NTP, ani="0")
    df_NTP_cat = T_add_columns(df=df_NTP_cat, data_dict=data_dict_NTP, ani="1")
    df_TPE_dog = T_add_columns(df=df_TPE_dog, data_dict=data_dict_TPE, ani="0")
    df_TPE_cat = T_add_columns(df=df_TPE_cat, data_dict=data_dict_TPE, ani="1")
    df_TYN_dog = T_add_columns(df=df_TYN_dog, data_dict=data_dict_TYN, ani="0")
    df_TYN_cat = T_add_columns(df=df_TYN_cat, data_dict=data_dict_TYN, ani="1")
    df_TCH_dog = T_add_columns(df=df_TCH_dog, data_dict=data_dict_TCH, ani="0")
    df_TCH_cat = T_add_columns(df=df_TCH_cat, data_dict=data_dict_TCH, ani="1")
    df_TNA_dog = T_add_columns(df=df_TNA_dog, data_dict=data_dict_TNA, ani="0")
    df_TNA_cat = T_add_columns(df=df_TNA_cat, data_dict=data_dict_TNA, ani="1")
    df_KSH_dog = T_add_columns(df=df_KSH_dog, data_dict=data_dict_KSH, ani="0")
    df_KSH_cat = T_add_columns(df=df_KSH_cat, data_dict=data_dict_KSH, ani="1")

    df_NTP = pdm.T_combine_dataframe(df1=df_NTP_dog, df2=df_NTP_cat)
    df_TPE = pdm.T_combine_dataframe(df1=df_TPE_dog, df2=df_TPE_cat)
    df_TYN = pdm.T_combine_dataframe(df1=df_TYN_dog, df2=df_TYN_cat)
    df_TCH = pdm.T_combine_dataframe(df1=df_TCH_dog, df2=df_TCH_cat)
    df_TNA = pdm.T_combine_dataframe(df1=df_TNA_dog, df2=df_TNA_cat)
    df_KSH = pdm.T_combine_dataframe(df1=df_KSH_dog, df2=df_KSH_cat)

    L_save_file_to_csv(
        folder=data_dict_NTP["folder"], file_name=data_dict_NTP["file_name"], df=df_NTP)
    L_save_file_to_csv(
        folder=data_dict_TPE["folder"], file_name=data_dict_TPE["file_name"], df=df_TPE)
    L_save_file_to_csv(
        folder=data_dict_TYN["folder"], file_name=data_dict_TYN["file_name"], df=df_TYN)
    L_save_file_to_csv(
        folder=data_dict_TCH["folder"], file_name=data_dict_TCH["file_name"], df=df_TCH)
    L_save_file_to_csv(
        folder=data_dict_TNA["folder"], file_name=data_dict_TNA["file_name"], df=df_TNA)
    L_save_file_to_csv(
        folder=data_dict_KSH["folder"], file_name=data_dict_KSH["file_name"], df=df_KSH)

    df_main = pdm.T_combine_six_dataframe(
        df1=df_NTP,
        df2=df_TPE,
        df3=df_TYN,
        df4=df_TCH,
        df5=df_TNA,
        df6=df_KSH,
    )


d_01_pet_regis_count()
