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
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="d_01-1_pet_regis_count_daily",
    default_args=default_args,
    description="[每日更新]爬取每日寵物登記數",
    schedule_interval="0 20 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "daily", "registration"]
)
def d_01_pet_regis_count():
    @task
    def S_get_requests_data_dict(city_dict: dict, city_index: int) -> dict:
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
            "folder": "/opt/airflow/data/raw/registrue",
            "file_name": f"{city_name}.csv"
        }
        return data_dict

    @task
    def S_create_post_data(dict_name: dict, ani: str) -> dict:
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
        res = requests.post(url=url, headers=headers, data=data)
        res.raise_for_status()
        res.encoding = "utf-8-sig"

        # 因資料是以json格式儲存和回傳，故需json解碼
        data_orig = json.loads(res.text)
        data_str = data_orig.get("Message", "[]")
        data_json = json.loads(data_str)

        return data_json

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def E_get_main_data(data_dict: dict, data: dict) -> pd.DataFrame:
        data_json = post_requests(
            url=data_dict["url"], headers=data_dict["headers"], data=data)
        df = pd.DataFrame(data_json)
        time.sleep(5)

        return df

    @task
    def T_add_columns(df: pd.DataFrame, data_dict: dict, ani: str) -> pd.DataFrame:
        df["date"] = data_dict["start_date"]
        df["animal"] = ani
        df["city"] = data_dict["city"]
        df["update_date"] = data_dict["end_date"]

        return df

    @task
    def S_get_save_setting(dict_name: dict) -> dict:
        folder = dict_name["folder"]
        file_name = dict_name["file_name"]

        return {"folder": folder, "file_name": file_name}

    @task
    def T_trans_city_to_ch(df: pd.DataFrame, city_dict: dict) -> pd.DataFrame:
        df["city"] = df["city"].apply(lambda x: city_dict[x])

        return df

    @task
    def T_clean_district_value(df: pd.DataFrame) -> pd.DataFrame:
        df["district"] = df["district"].apply(lambda x: x[3:])

        return df

    @task
    def T_df_merge_location(df_main: pd.DataFrame, df_loc: list[dict]) -> pd.DataFrame:
        df_loc = pd.DataFrame(df_loc)
        df_loc = df_loc[["loc_id", "city", "district"]]

        df_main = df_main.merge(df_loc, how="left", on=["city", "district"])
        df_main.drop(columns=["city", "district"], axis=1, inplace=True)

        return df_main

    @task
    def L_complete_save_file(df: pd.DataFrame):
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
        file_date = date.today().strftime("%Y-%m-%d")
        bucket_name = "tjr103-1-project-bucket"
        destination = f"data/complete/registration/dt={file_date}/registration.csv"
        source_file_name = f"/opt/airflow/data/complete/registrue/dt={file_date}/registration.csv"

        return {
            "bucket_name": bucket_name,
            "destination": destination,
            "source_file_name": source_file_name
        }

    """程式正式開始"""

    # 取得六都與代碼對照表
    city_dict = cfg.CITY_NAME_CODE_DICT

    # 建立六都的爬蟲資料表
    data_dict_NTP = S_get_requests_data_dict(city_dict=city_dict, city_index=0)
    data_dict_TPE = S_get_requests_data_dict(city_dict=city_dict, city_index=1)
    data_dict_TYN = S_get_requests_data_dict(city_dict=city_dict, city_index=2)
    data_dict_TCH = S_get_requests_data_dict(city_dict=city_dict, city_index=3)
    data_dict_TNA = S_get_requests_data_dict(city_dict=city_dict, city_index=4)
    data_dict_KSH = S_get_requests_data_dict(city_dict=city_dict, city_index=5)

    # 建立六都貓、狗的post資料表
    data_NTP_dog = S_create_post_data(dict_name=data_dict_NTP, ani="0")
    data_NTP_cat = S_create_post_data(dict_name=data_dict_NTP, ani="1")
    data_TPE_dog = S_create_post_data(dict_name=data_dict_TPE, ani="0")
    data_TPE_cat = S_create_post_data(dict_name=data_dict_TPE, ani="1")
    data_TYN_dog = S_create_post_data(dict_name=data_dict_TYN, ani="0")
    data_TYN_cat = S_create_post_data(dict_name=data_dict_TYN, ani="1")
    data_TCH_dog = S_create_post_data(dict_name=data_dict_TCH, ani="0")
    data_TCH_cat = S_create_post_data(dict_name=data_dict_TCH, ani="1")
    data_TNA_dog = S_create_post_data(dict_name=data_dict_TNA, ani="0")
    data_TNA_cat = S_create_post_data(dict_name=data_dict_TNA, ani="1")
    data_KSH_dog = S_create_post_data(dict_name=data_dict_KSH, ani="0")
    data_KSH_cat = S_create_post_data(dict_name=data_dict_KSH, ani="1")

    # 逐一爬取六都貓狗資料
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

    # 將表加上動物及一些紀錄欄位
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

    # 將各都的貓狗df合併
    df_NTP = pdm.T_combine_dataframe(df1=df_NTP_dog, df2=df_NTP_cat)
    df_TPE = pdm.T_combine_dataframe(df1=df_TPE_dog, df2=df_TPE_cat)
    df_TYN = pdm.T_combine_dataframe(df1=df_TYN_dog, df2=df_TYN_cat)
    df_TCH = pdm.T_combine_dataframe(df1=df_TCH_dog, df2=df_TCH_cat)
    df_TNA = pdm.T_combine_dataframe(df1=df_TNA_dog, df2=df_TNA_cat)
    df_KSH = pdm.T_combine_dataframe(df1=df_KSH_dog, df2=df_KSH_cat)

    # 取得六都存檔設定
    NTP_save_setting = S_get_save_setting(dict_name=data_dict_NTP)
    TPE_save_setting = S_get_save_setting(dict_name=data_dict_TPE)
    TYN_save_setting = S_get_save_setting(dict_name=data_dict_TYN)
    TCH_save_setting = S_get_save_setting(dict_name=data_dict_TCH)
    TNA_save_setting = S_get_save_setting(dict_name=data_dict_TNA)
    KSH_save_setting = S_get_save_setting(dict_name=data_dict_KSH)

    # 將六都合併後的df先存檔紀錄
    dfm.L_save_file_to_csv_by_dict(save_setting=NTP_save_setting, df=df_NTP)
    dfm.L_save_file_to_csv_by_dict(save_setting=TPE_save_setting, df=df_TPE)
    dfm.L_save_file_to_csv_by_dict(save_setting=TYN_save_setting, df=df_TYN)
    dfm.L_save_file_to_csv_by_dict(save_setting=TCH_save_setting, df=df_TCH)
    dfm.L_save_file_to_csv_by_dict(save_setting=TNA_save_setting, df=df_TNA)
    dfm.L_save_file_to_csv_by_dict(save_setting=KSH_save_setting, df=df_KSH)

    # 將六都df合併為單一主表
    df_main = pdm.T_combine_six_dataframe(
        df1=df_NTP,
        df2=df_TPE,
        df3=df_TYN,
        df4=df_TCH,
        df5=df_TNA,
        df6=df_KSH,)

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

    # 連線資料庫取得location資料
    df_loc = dfm.E_load_from_sql(table_name="location")

    # 與location表合併，並留下loc_id
    df_main = T_df_merge_location(df_main=df_main, df_loc=df_loc)

    # 重新排序欄位
    df_main = pdm.T_sort_columns(
        df=df_main, new_cols=cfg.PET_REGIS_FINAL_COLUMNS)

    # 存檔至地端
    save = L_complete_save_file(df=df_main)

    # 將更新資料輸入資料庫
    sql = "INSERT INTO pet_regis (loc_id, date, animal, regis_count, removal_count, update_date)" \
        "VALUES(%s, %s, %s, %s, %s, %s)"

    dfm.L_upload_data_to_db(df=df_main, sql=sql)

    # 取得GCS存檔設定
    gcs_setting = S_get_gcs_setting()

    # 上傳至GCS
    gcs.L_upload_to_gcs(gcs_setting=gcs_setting)

    save >> gcs_setting


d_01_pet_regis_count()
