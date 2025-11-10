import ast
import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from shapely.geometry import Point
from sqlalchemy import create_engine
from pathlib import Path
from utils.config import STORE_TYPE_ENG_CH_DICT, GSEARCH_CITY_CODE
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


# @dag(
#     dag_id="d_03_gmap_filter_place",
#     default_args=default_args,
#     description="[資料處理]將在六都邊界外的資料去除，減少後續查詢次數",
#     schedule_interval="0 */2 * * *",
#     start_date=datetime(2023, 1, 1),
#     catchup=False,
#     # Optional: Add tags for better filtering in the UI
#     tags=["bevis", "cleaning"]
# )
def d_03_gmap_filter_place():
    def S_get_gdf() -> gpd.GeoDataFrame:
        folder = Path("/opt/airflow/utils")
        file_name = "COUNTY_MOI_1140318.gml"
        path = folder / file_name

        gdf = gpd.read_file(path)

        return gdf

    def S_get_city_geodata(city_name: str):
        gdf = S_get_gdf()
        city_idx = gdf[gdf["名稱"] == city_name].index
        geo_data = gdf.loc[city_idx].geometry.values[0]

        return geo_data

    def in_boundary(city_geo_data, lat, lon):
        loc_p = Point(lon, lat)
        return city_geo_data.contains(loc_p)

    def S_get_keyword_dict(store_type_dict: dict, index: int):
        keyword_list = list(store_type_dict.keys())
        type_list = list(store_type_dict.values())

        return {"ch_keyword": keyword_list[index], "keyword": type_list[index]}

    @task
    def S_get_city_dict(gsearch_dict: dict, index: int):
        city_name_list = list(gsearch_dict.keys())
        city_code_list = list(gsearch_dict.values())
        return {"city_name": city_name_list[index], "city_code": city_code_list[index]}

    @task
    def E_read_df_file(city_dict: dict, keyword_dict: str):
        folder = Path(f"/opt/airflow/data/raw/{keyword_dict['keyword']}")
        file_name = f"{city_dict['city_code']}_{keyword_dict['keyword']}.csv"
        path = folder / file_name
        df = pd.read_csv(path)

        return df

    @task
    def T_keep_operation_store(df: pd.DataFrame) -> pd.DataFrame:
        mask = (df["buss_status"] == "OPERATIONAL")
        df = df[mask]

        return df

    @task
    def T_drop_duplicated(df: pd.DataFrame) -> pd.DataFrame:
        df = df.drop_duplicates(subset=["place_id"], keep="first")

        return df

    @task
    def T_drop_no_geometry(df: pd.DataFrame) -> pd.DataFrame:
        df = df.dropna(subset=["geometry"])

        return df

    @task
    def T_detect_in_boundary_or_not(df: pd.DataFrame, city_dict: dict) -> pd.DataFrame:
        geo_data = S_get_city_geodata(city_name=city_dict["city_name"])
        boundary_list = []
        df["geometry"] = df["geometry"].apply(ast.literal_eval)
        for index, row in df.iterrows():
            lat = row["geometry"].get("lat", None)
            lon = row["geometry"].get("lng", None)
            if lat == None or lon == None:
                boundary_list.append(False)
            else:
                boundary_list.append(in_boundary(
                    city_geo_data=geo_data, lat=lat, lon=lon))

        df["in_boundary"] = boundary_list

        return df

    @task
    def T_drop_data_out_boundary(df: pd.DataFrame) -> pd.DataFrame:
        mask = (df["in_boundary"] == True)
        df = df[mask]

        return df

    @task
    def T_add_update_date(df: pd.DataFrame) -> pd.DataFrame:
        today = date.today().strftime('%Y/%m/%d')
        df["update_date"] = today

        return df

    def S_get_save_setting(keyword_dict: dict) -> dict:
        folder = f"/opt/airflow/data/processed/{keyword_dict['keyword']}"
        file_name = f"{keyword_dict['keyword']}_place_id.csv"

        return {"folder": folder, "file_name": file_name}

    """程式正式開始"""

    # 選擇本程式處理的商店類別
    # 0為寵物美容、1為寵物餐廳、2為寵物用品
    keyword_dict = S_get_keyword_dict(
        store_type_dict=STORE_TYPE_ENG_CH_DICT, index=0)

    # 取得六都中文名及代號
    # 0為新北（範圍包含台北）、1為桃園、2為臺中、3為臺南、4為高雄
    TPE_city_dict = S_get_city_dict(gsearch_dict=GSEARCH_CITY_CODE, index=0)
    TYU_city_dict = S_get_city_dict(gsearch_dict=GSEARCH_CITY_CODE, index=1)
    TCH_city_dict = S_get_city_dict(gsearch_dict=GSEARCH_CITY_CODE, index=2)
    TNA_city_dict = S_get_city_dict(gsearch_dict=GSEARCH_CITY_CODE, index=3)
    KSH_city_dict = S_get_city_dict(gsearch_dict=GSEARCH_CITY_CODE, index=4)

    # 讀取六都檔案
    df_TPE = E_read_df_file(city_dict=TPE_city_dict, keyword_dict=keyword_dict)
    df_TYU = E_read_df_file(city_dict=TYU_city_dict, keyword_dict=keyword_dict)
    df_TCH = E_read_df_file(city_dict=TCH_city_dict, keyword_dict=keyword_dict)
    df_TNA = E_read_df_file(city_dict=TNA_city_dict, keyword_dict=keyword_dict)
    df_KSH = E_read_df_file(city_dict=KSH_city_dict, keyword_dict=keyword_dict)

    # 簡單清理檔案
    # 去除place id重複資料
    df_TPE = T_drop_duplicated(df=df_TPE)
    df_TYU = T_drop_duplicated(df=df_TYU)
    df_TCH = T_drop_duplicated(df=df_TCH)
    df_TNA = T_drop_duplicated(df=df_TNA)
    df_KSH = T_drop_duplicated(df=df_KSH)

    # 去除非正常營業資料
    df_TPE = T_keep_operation_store(df=df_TPE)
    df_TYU = T_keep_operation_store(df=df_TYU)
    df_TCH = T_keep_operation_store(df=df_TCH)
    df_TNA = T_keep_operation_store(df=df_TNA)
    df_KSH = T_keep_operation_store(df=df_KSH)

    # 去除沒有地理資料的店家
    df_TPE = T_drop_no_geometry(df=df_TPE)
    df_TYU = T_drop_no_geometry(df=df_TYU)
    df_TCH = T_drop_no_geometry(df=df_TCH)
    df_TNA = T_drop_no_geometry(df=df_TNA)
    df_KSH = T_drop_no_geometry(df=df_KSH)

    # 根據地理資訊查詢是否真的在六都邊界內
    df_TPE = T_detect_in_boundary_or_not(df=df_TPE, city_dict=TPE_city_dict)
    df_TYU = T_detect_in_boundary_or_not(df=df_TYU, city_dict=TYU_city_dict)
    df_TCH = T_detect_in_boundary_or_not(df=df_TCH, city_dict=TCH_city_dict)
    df_TNA = T_detect_in_boundary_or_not(df=df_TNA, city_dict=TNA_city_dict)
    df_KSH = T_detect_in_boundary_or_not(df=df_KSH, city_dict=KSH_city_dict)

    # 去除不在邊界內的資料
    df_TPE = T_drop_data_out_boundary(df=df_TPE)
    df_TYU = T_drop_data_out_boundary(df=df_TYU)
    df_TCH = T_drop_data_out_boundary(df=df_TCH)
    df_TNA = T_drop_data_out_boundary(df=df_TNA)
    df_KSH = T_drop_data_out_boundary(df=df_KSH)

    # 將五個df合併
    df_main = pdm.T_combine_five_dataframe(
        df1=df_TPE,
        df2=df_TYU,
        df3=df_TCH,
        df4=df_TNA,
        df5=df_KSH
    )

    # 合併後再次去除重複place id資料
    df_main = T_drop_duplicated(df=df_main)

    # 新增更新日期
    df_main = T_add_update_date(df=df_main)

    # 取得存檔設定
    save_setting = S_get_save_setting(keyword_dict=keyword_dict)

    # 存檔至地端
    dfm.L_save_file_to_csv_by_dict(save_setting=save_setting, df=df_main)


# d_03_gmap_filter_place()
