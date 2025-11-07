import ast
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from dotenv import load_dotenv
from shapely.geometry import Point
from tasks import database_file_mod as dfm
from tasks import pandas_mod as pdm
from utils import gmap_mod as gm
from utils.config import GSEARCH_CITY_CODE, STORE_TYPE_ENG_CH_DICT


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
    dag_id="d_02-2_gmap_full_search_restaurant",
    default_args=default_args,
    description="[每月更新]透過經緯度爬取六都「寵物美容」列表",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "monthly", "restaurant", "test_done"]
)
def d_02_2_gmap_full_search_restaurant():

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

    @task
    def S_get_city_data(dict_name: dict, index: int) -> dict:
        city_name_list = list(dict_name.keys())
        city_code_list = list(dict_name.values())
        return {"city_name": city_name_list[index], "city_code": city_code_list[index]}

    @task
    def S_get_keyword_dict(dict_name: dict, index: int):
        keyword_list = list(dict_name.keys())
        file_name_list = list(dict_name.values())
        return {
            "keyword": keyword_list[index], "file_name": file_name_list[index]
        }

    def S_search_setting(radius: int, step: int):
        return {
            "radius": radius,
            "step": step
        }

    @task
    def S_set_loc_point(search_setting: dict, city_name: str) -> list:
        geo_data = S_get_city_geodata(city_name=city_name)

        step_m = search_setting["step"]
        min_x, min_y, max_x, max_y = geo_data.bounds

        lat_step = step_m / 111000
        lon_step = step_m / (111000 * np.cos(np.radians((min_y + max_y) / 2)))

        lat_point = np.arange(min_y, max_y, lat_step)
        lon_point = np.arange(min_x, max_x, lon_step)

        loc_points = []
        for lat_p in lat_point:
            for lon_p in lon_point:
                loc = (lon_p, lat_p)
                loc_points.append(loc)

        print(f"總座標數：{len(loc_points)}")

        return loc_points

    @task
    def E_gmap_search(
            city_data: dict,
            keyword: dict,
            search_setting: dict,
            loc_points: list,
    ):

        geo_data = S_get_city_geodata(city_name=city_data["city_name"])

        load_dotenv()
        data = []
        key = os.environ.get("GMAP_KEY6")
        count = 1

        for loc in loc_points:
            if len(data) >= 5:
                break

            loc_p = Point(loc)
            if geo_data.contains(loc_p):
                lat = loc[1]
                lon = loc[0]
                result_list = gm.gmap_nearby_search(
                    key=key,
                    lat=lat,
                    lon=lon,
                    radius=search_setting["radius"],
                    keyword=keyword["keyword"]
                )
                data.extend(result_list)
                print(
                    f"完成{city_data['city_name']}的{keyword['keyword']}的第{count}/{len(loc_points)}個座標點（{round(float(loc[1]), 7)}, {round(float(loc[0]), 7)}）的搜尋，共有{len(result_list)}筆店家資料")

                df = pd.DataFrame(data=data)
                df["update_time"] = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

                folder = Path(
                    f"/opt/airflow/data/raw/{keyword['file_name']}/temp")  # 容器路徑
                folder.mkdir(parents=True, exist_ok=True)
                file_name = f"{city_data['city_code']}_{keyword['file_name']}_temp.csv"
                path = folder / file_name
                df.to_csv(path, index=False, encoding="utf-8-sig")
                count += 1
                time.sleep(1.5)

        metadata = {
            "city": city_data["city_name"],
            "search_radius": search_setting["radius"],
            "step": search_setting["step"],
            "coord_count": count,
            "data_count": len(data),
            "type": keyword["keyword"],
            "update_date": date.today().strftime("%Y/%m/%d")
        }

        return {"data": data, "metadata": metadata}

    @task
    def T_transform_to_df(result_dict: dict) -> pd.DataFrame:
        data = result_dict["data"]
        df = pd.DataFrame(data=data)
        df["update_time"] = datetime.now().strftime("%Y/%m/%d %H:%M:%S")

        return df

    @task
    def T_transform_metadata_df(result_dict: dict) -> pd.DataFrame:
        data = result_dict["metadata"]
        df = pd.DataFrame(data=data, index=[0])

        return df

    @task
    def S_get_save_setting(keyword_dict: dict, city_dict: dict):
        folder = f"/opt/airflow/data/raw/{keyword_dict['file_name']}"
        file_name = f"{city_dict['city_code']}_{keyword_dict['file_name']}.csv"
        return {"folder": folder, "file_name": file_name}

    def S_get_metadata_save_setting():
        file_date = date.today().strftime('%Y%m%d')
        folder = "/opt/airflow/data/complete/gmap_record"
        file_name = f"{file_date}_gmap_record.csv"

        return {"folder": folder, "file_name": file_name}

    def in_boundary(city_geo_data, lat, lon):
        loc_p = Point(lon, lat)
        return city_geo_data.contains(loc_p)

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

    def to_dict_if_str(object):
        if isinstance(object, str):
            return ast.literal_eval(object)
        else:
            return object

    @task
    def T_detect_in_boundary_or_not(df: pd.DataFrame, city_dict: dict) -> pd.DataFrame:
        geo_data = S_get_city_geodata(city_name=city_dict["city_name"])
        boundary_list = []
        df["geometry"] = df["geometry"].apply(to_dict_if_str)
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

    def S_get_main_save_setting(keyword_dict: dict) -> dict:
        folder = f"/opt/airflow/data/processed/{keyword_dict['file_name']}"
        file_name = f"{keyword_dict['file_name']}_place_id.csv"

        return {"folder": folder, "file_name": file_name}

    """程式正式開始"""

    # 爬取的商店類型，若要修改則在此變更。
    # 0為寵物美容，1為寵物餐廳，2為寵物用品
    keyword_dict = S_get_keyword_dict(
        dict_name=STORE_TYPE_ENG_CH_DICT, index=1)

    # 將六都及對應的字串名稱取出
    TPE_city_dict = S_get_city_data(dict_name=GSEARCH_CITY_CODE, index=0)
    TYU_city_dict = S_get_city_data(dict_name=GSEARCH_CITY_CODE, index=1)
    TCH_city_dict = S_get_city_data(dict_name=GSEARCH_CITY_CODE, index=2)
    TNA_city_dict = S_get_city_data(dict_name=GSEARCH_CITY_CODE, index=3)
    KSH_city_dict = S_get_city_data(dict_name=GSEARCH_CITY_CODE, index=4)

    # 設定搜尋參數：半徑與步長
    search_setting = S_search_setting(radius=3000, step=3000)

    # 根據搜尋參數設定與六都邊界資料，列出所有座標點
    TPE_loc_points = S_set_loc_point(
        search_setting=search_setting, city_name=TPE_city_dict["city_name"])
    TYU_loc_points = S_set_loc_point(
        search_setting=search_setting, city_name=TYU_city_dict["city_name"])
    TCH_loc_points = S_set_loc_point(
        search_setting=search_setting, city_name=TCH_city_dict["city_name"])
    TNA_loc_points = S_set_loc_point(
        search_setting=search_setting, city_name=TNA_city_dict["city_name"])
    KSH_loc_points = S_set_loc_point(
        search_setting=search_setting, city_name=KSH_city_dict["city_name"])

    # 正式使用gmap開始搜尋
    TPE_result_dict = E_gmap_search(city_data=TPE_city_dict, keyword=keyword_dict,
                                    search_setting=search_setting, loc_points=TPE_loc_points)
    TYU_result_dict = E_gmap_search(city_data=TYU_city_dict, keyword=keyword_dict,
                                    search_setting=search_setting, loc_points=TYU_loc_points)
    TCH_result_dict = E_gmap_search(city_data=TCH_city_dict, keyword=keyword_dict,
                                    search_setting=search_setting, loc_points=TCH_loc_points)
    TNA_result_dict = E_gmap_search(city_data=TNA_city_dict, keyword=keyword_dict,
                                    search_setting=search_setting, loc_points=TNA_loc_points)
    KSH_result_dict = E_gmap_search(city_data=KSH_city_dict, keyword=keyword_dict,
                                    search_setting=search_setting, loc_points=KSH_loc_points)

    # 將搜尋結果轉換成六都df
    df_TPE = T_transform_to_df(result_dict=TPE_result_dict)
    df_TYU = T_transform_to_df(result_dict=TYU_result_dict)
    df_TCH = T_transform_to_df(result_dict=TCH_result_dict)
    df_TNA = T_transform_to_df(result_dict=TNA_result_dict)
    df_KSH = T_transform_to_df(result_dict=KSH_result_dict)

    # 取得六都的存檔設定
    TPE_save_setting = S_get_save_setting(
        keyword_dict=keyword_dict, city_dict=TPE_city_dict)
    TYU_save_setting = S_get_save_setting(
        keyword_dict=keyword_dict, city_dict=TYU_city_dict)
    TCH_save_setting = S_get_save_setting(
        keyword_dict=keyword_dict, city_dict=TCH_city_dict)
    TNA_save_setting = S_get_save_setting(
        keyword_dict=keyword_dict, city_dict=TNA_city_dict)
    KSH_save_setting = S_get_save_setting(
        keyword_dict=keyword_dict, city_dict=KSH_city_dict)

    # 將六都df存檔成csv
    dfm.L_save_file_to_csv_by_dict(save_setting=TPE_save_setting, df=df_TPE)
    dfm.L_save_file_to_csv_by_dict(save_setting=TYU_save_setting, df=df_TYU)
    dfm.L_save_file_to_csv_by_dict(save_setting=TCH_save_setting, df=df_TCH)
    dfm.L_save_file_to_csv_by_dict(save_setting=TNA_save_setting, df=df_TNA)
    dfm.L_save_file_to_csv_by_dict(save_setting=KSH_save_setting, df=df_KSH)

    # 取得gmap的metadata紀錄並轉成df
    df_meta_TPE = T_transform_metadata_df(result_dict=TPE_result_dict)
    df_meta_TYU = T_transform_metadata_df(result_dict=TYU_result_dict)
    df_meta_TCH = T_transform_metadata_df(result_dict=TCH_result_dict)
    df_meta_TNA = T_transform_metadata_df(result_dict=TNA_result_dict)
    df_meta_KSH = T_transform_metadata_df(result_dict=KSH_result_dict)

    # 將metadata的df合併
    df_metadata = pdm.T_combine_five_dataframe(
        df1=df_meta_TPE,
        df2=df_meta_TYU,
        df3=df_meta_TCH,
        df4=df_meta_TNA,
        df5=df_meta_KSH)

    # 取得metadata存檔資訊
    metadata_save_setting = S_get_metadata_save_setting()

    # 將metadata的紀錄存檔成csv
    dfm.L_save_file_to_csv_by_dict(
        save_setting=metadata_save_setting, df=df_metadata)

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
    main_save_setting = S_get_main_save_setting(keyword_dict=keyword_dict)

    # 存檔至地端
    dfm.L_save_file_to_csv_by_dict(save_setting=main_save_setting, df=df_main)


d_02_2_gmap_full_search_restaurant()
