import ast
import os
import time
from datetime import date, datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from dotenv import load_dotenv
from shapely.geometry import Point
from utils import gmap_mod as gm
from utils.config import GSEARCH_CITY_CODE, STORE_TYPE_ENG_CH_DICT


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


@task
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


@task
def S_get_main_save_setting(keyword_dict: dict) -> dict:
    folder = f"/opt/airflow/data/processed/{keyword_dict['file_name']}"
    file_name = f"{keyword_dict['file_name']}_place_id.csv"

    return {"folder": folder, "file_name": file_name}
