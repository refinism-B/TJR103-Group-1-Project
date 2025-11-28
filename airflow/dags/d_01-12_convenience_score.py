from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from tasks import GCS_mod as gcs
from tasks import database_file_mod as dfm
from tasks import pandas_mod as pdm
from utils.config import (CATEGORY_WEIGHTED_DICT, CONVENIENCE_SCORE_COLUMNS,
                          STORE_STORE_COLUMNS)

# 設定DAG基本資訊
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["add412@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="d_01-12_convenience_score",
    default_args=default_args,
    description="[每週]更新便利性指標報表",
    schedule_interval="0 10 * * 1",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "weekly", "convenience_score", "analysis"]
)
def d_01_12_convenience_score():
    @task
    def T_add_city_id(df_data: pd.DataFrame) -> pd.DataFrame:
        """加上city_id欄位，後續部分計算是基於「市」的分類"""
        df = pd.DataFrame(df_data)
        df = df.dropna(subset="loc_id")
        df["city_id"] = df["loc_id"].str[:3]

        return df.to_dict(orient='records')

    @task
    def T_calculate_w_area_cat(df: pd.DataFrame, t: int = 30) -> pd.DataFrame:
        """計算w值"""
        df = pd.DataFrame(data=df)

        df[["loc_id", "category_id"]] = df[[
            "loc_id", "category_id"]].astype(str)

        loc_store_count = df.groupby(
            ["loc_id", "category_id"]).size().reset_index(name="store_count")

        loc_store_count["w_area_cat"] = loc_store_count["store_count"] / \
            (loc_store_count["store_count"] + t)

        df = df.merge(loc_store_count, how="left",
                      on=["loc_id", "category_id"])

        return df.to_dict(orient='records')

    @task
    def T_calculate_P75_score(df: pd.DataFrame) -> pd.DataFrame:
        """計算P75分數"""
        df = pd.DataFrame(data=df)

        # 計算P75_city_area_cat
        P75_district_rating_total = df.groupby(["loc_id", "category_id"])[
            "rating_total"].quantile(0.75).reset_index(name="P75_district_rating_total")

        # 計算P75_city_cat
        P75_city_rating_total = df.groupby(["city_id", "category_id"])[
            "rating_total"].quantile(0.75).reset_index(name="P75_city_rating_total")

        # merge回店家總表
        df = df.merge(P75_district_rating_total, how="left",
                      on=["loc_id", "category_id"])
        df = df.merge(P75_city_rating_total, how="left",
                      on=["city_id", "category_id"])

        return df.to_dict(orient='records')

    @task
    def T_calculate_m_score(df: pd.DataFrame) -> pd.DataFrame:
        """計算m值"""
        df = pd.DataFrame(data=df)

        # 計算m_city_area_cat
        df["m_city_area_cat"] = (df["w_area_cat"] * df["P75_district_rating_total"]) + (
            (1 - df["w_area_cat"]) * df["P75_city_rating_total"])

        return df.to_dict(orient='records')

    @task
    def T_calculate_rating_avg(df: pd.DataFrame) -> pd.DataFrame:
        """計算各類、各區的平均評分"""
        df = pd.DataFrame(data=df)

        district_rating_avg = df.groupby(["loc_id", "category_id"])[
            "rating"].mean().reset_index(name="district_rating_avg")
        df = df.merge(district_rating_avg, how="left",
                      on=["loc_id", "category_id"])

        return df.to_dict(orient='records')

    @task
    def T_calculate_avb_score(df: pd.DataFrame) -> pd.DataFrame:
        """根據營業時數換算available分數"""
        df = pd.DataFrame(data=df)

        df["avb_score"] = ((df["op_hours"]/168)*0.5) + 0.5

        return df.to_dict(orient='records')

    @task
    def T_calculate_store_score(df: pd.DataFrame, new_col_sort: list) -> pd.DataFrame:
        """計算單店家分數"""
        df = pd.DataFrame(data=df)

        df["store_score"] = (((df["rating"]/5) * (df["rating_total"]/(df["rating_total"]+df["m_city_area_cat"]))) + (
            (df["district_rating_avg"]/5)*(df["m_city_area_cat"]/(df["m_city_area_cat"]+df["rating_total"])))) * df["avb_score"]
        df = df[new_col_sort]

        return df.to_dict(orient='records')

    @task
    def T_calculate_pet_count(df: pd.DataFrame) -> pd.DataFrame:
        """將各區的寵物數量加總計算"""
        df_pet_count = df.groupby("loc_id").agg(
            regis=("regis_count", "sum"),
            removal=("removal_count", "sum")
        ).reset_index()
        df_pet_count["pet_count"] = df_pet_count["regis"] - \
            df_pet_count["removal"]

        return df_pet_count

    @task
    def T_calculate_category_raw_score(df_pet_count: pd.DataFrame, df_main: pd.DataFrame) -> pd.DataFrame:
        """計算各區類別的raw分數"""
        df_main = pd.DataFrame(data=df_main)

        sum_store_score = df_main.groupby(["loc_id", "category_id"])[
            "store_score"].sum().reset_index(name="sum_store_score")

        df_category_score = sum_store_score.merge(
            df_pet_count, how="left", on="loc_id")
        df_category_score["category_raw_score"] = df_category_score["sum_store_score"] / (
            df_category_score["pet_count"]/10000)
        df_category_score["category_raw_score"] = df_category_score["category_raw_score"].fillna(
            0)

        df_category_score["city_id"] = df_category_score["loc_id"].str[:3]

        df_category_score

        return df_category_score.to_dict(orient='records')

    def normalize_series(x: pd.Series, p10: pd.Series, p90: pd.Series) -> pd.Series:
        """
        x   : 要轉換的原始分數（Series）
        p10 : 同長度的第10百分位數（Series，已對齊 x）
        p90 : 同長度的第90百分位數（Series，已對齊 x）
        回傳：回傳到 0.5-9.5 的分數
        """
        # 避免 P90==P10 造成除0，先把相等的分母換成NaN
        denom = (p90 - p10).replace(0, pd.NA)
        ratio = (x - p10) / denom
        # 若分母為NaN（等於 0 的情況），或原本就NaN視為0
        ratio = ratio.fillna(0.0)
        # 夾在 [0, 1]
        ratio = ratio.clip(0, 1)
        # 回傳到 [0.5, 9.5]
        return 0.5 + ratio * 9.0

    @task
    def T_get_normalize_score(df: pd.DataFrame, col_list: list, col_name: str) -> pd.DataFrame:
        """計算各區各類的標準化分數，並分為市內比較及全國比較"""
        df_copy = pd.DataFrame(df)

        group = df_copy.groupby(col_list)["category_raw_score"]
        p10 = group.transform(lambda s: s.quantile(0.10))
        p90 = group.transform(lambda s: s.quantile(0.90))
        df_copy[col_name] = normalize_series(
            x=df_copy["category_raw_score"], p10=p10, p90=p90)
        df_copy[col_name] = df_copy[col_name].round(2)

        return df_copy.to_dict(orient='records')

    @task
    def T_merge_city_and_all(df_city: pd.DataFrame, df_all: pd.DataFrame) -> pd.DataFrame:
        """將市內和全國比較的標準化分數合併"""
        df_city = pd.DataFrame(data=df_city)
        df_all = pd.DataFrame(data=df_all)

        df_all = df_all[["loc_id", "category_id", "norm_all"]]
        df_final = df_city.merge(df_all, how="left", on=[
                                 "loc_id", "category_id"])

        return df_final.to_dict(orient='records')

    @task
    def T_calculate_category_score(df: pd.DataFrame, weighted_dict: dict) -> pd.DataFrame:
        """計算各區的類別分數"""
        df = pd.DataFrame(data=df)

        df["category_city_score"] = df["norm_city"] * \
            df["category_id"].map(weighted_dict)
        df["category_all_score"] = df["norm_all"] * \
            df["category_id"].map(weighted_dict)

        return df.to_dict(orient='records')

    @task
    def T_calculate_city_all_score(df: pd.DataFrame) -> pd.DataFrame:
        """計算各區市內和全國比較的總分"""
        df = pd.DataFrame(data=df)

        df_city_score = df.groupby(
            "loc_id")["category_city_score"].sum().reset_index(name="city_score")
        df = df.merge(df_city_score, how="left", on="loc_id")
        df_all_score = df.groupby(
            "loc_id")["category_all_score"].sum().reset_index(name="all_score")
        df = df.merge(df_all_score, how="left", on="loc_id")

        return df.to_dict(orient='records')

    @task
    def T_calculate_final_score(df: pd.DataFrame) -> pd.DataFrame:
        """以市內比較7:全國比較3的比重加總得到最終分數"""
        df = pd.DataFrame(data=df)

        df["final_score"] = (df["city_score"]*0.7) + (df["all_score"]*0.3)

        return df.to_dict(orient='records')

    @task
    def T_transform_final_to_10(df: pd.DataFrame, new_col: list) -> pd.DataFrame:
        """將最終分數映射回0-10分"""
        df = pd.DataFrame(data=df)

        df["final_score_10"] = df["final_score"] * (10/9.5)
        df["final_score_10"] = df["final_score_10"].round(2)

        df = df[new_col]

        return df.to_dict(orient='records')

    @task
    def S_get_store_score_save_setting():
        """取得單店分數總表的存檔位置"""
        folder = "/opt/airflow/data/complete/analysis"
        file_name = "store_score.csv"

        return {
            "folder": folder,
            "file_name": file_name
        }

    @task
    def S_get_store_score_gcs_setting(local_save_setting: dict):
        """取得單店分數總表上傳至GCS所需的路徑資訊"""
        folder = Path(local_save_setting["folder"])
        file_name = local_save_setting["file_name"]
        path = folder / file_name

        bucket_name = "tjr103-1-project-bucket"
        destination = f"data_analysis/customer/store_score.csv"
        source_file_name = str(path)

        return {
            "bucket_name": bucket_name,
            "destination": destination,
            "source_file_name": source_file_name
        }

    @task
    def S_get_convenience_score_save_setting():
        """取得便利性分數報表的存檔位置"""
        folder = "/opt/airflow/data/complete/analysis"
        file_name = "convenience_score.csv"

        return {
            "folder": folder,
            "file_name": file_name
        }

    @task
    def S_get_convenience_score_gcs_setting(local_save_setting: dict):
        """取得便利性分數報表上傳至GCS所需的路徑資訊"""
        folder = Path(local_save_setting["folder"])
        file_name = local_save_setting["file_name"]
        path = folder / file_name

        bucket_name = "tjr103-1-project-bucket"
        destination = f"data_analysis/customer/convenience_score.csv"
        source_file_name = str(path)

        return {
            "bucket_name": bucket_name,
            "destination": destination,
            "source_file_name": source_file_name
        }

    # 前置作業：先把所有店家表格讀入，並合併成主表
    data_salon = dfm.E_load_from_sql(table_name="salon")
    df_salon = pdm.T_transform_to_df(data=data_salon)

    data_hotel = dfm.E_load_from_sql(table_name="hotel")
    df_hotel = pdm.T_transform_to_df(data=data_hotel)

    data_hospital = dfm.E_load_from_sql(table_name="hospital")
    df_hospital = pdm.T_transform_to_df(data=data_hospital)

    data_supplies = dfm.E_load_from_sql(table_name="supplies")
    df_supplies = pdm.T_transform_to_df(data=data_supplies)

    data_restaurant = dfm.E_load_from_sql(table_name="restaurant")
    df_restaurant = pdm.T_transform_to_df(data=data_restaurant)

    data_shelter = dfm.E_load_from_sql(table_name="shelter")
    df_shelter = pdm.T_transform_to_df(data=data_shelter)

    df_main_data = pdm.T_combine_six_dataframe_out_dict(
        df1=df_salon,
        df2=df_hotel,
        df3=df_hospital,
        df4=df_supplies,
        df5=df_restaurant,
        df6=df_shelter
    )

    # 將寵物登記數表格讀入
    data_pet = dfm.E_load_from_sql(table_name="pet_regis")
    df_pet = pdm.T_transform_to_df(data=data_pet)

    # 建立city id欄位
    df_main = T_add_city_id(df_data=df_main_data)

    # 計算w_area_cat
    df_main = T_calculate_w_area_cat(df=df_main)

    # 計算P75_score
    df_main = T_calculate_P75_score(df=df_main)

    # 計算m_score
    df_main = T_calculate_m_score(df=df_main)

    # 計算地區平均評論數
    df_main = T_calculate_rating_avg(df=df_main)

    # 計算available score
    df_main = T_calculate_avb_score(df=df_main)

    # 計算單店家分數
    df_main = T_calculate_store_score(
        df=df_main, new_col_sort=STORE_STORE_COLUMNS)

    # 將單店分數總表存入DB
    dfm.L_truncate_and_upload_data_to_db(df=df_main, table_name="store_score")

    # 設定地端存檔資訊
    local_save_setting = S_get_store_score_save_setting()

    # 存檔至地端
    local_save = dfm.L_save_file_to_csv_by_dict(
        save_setting=local_save_setting, df=df_main)

    # 設定雲端存檔資訊
    gcs_save_setting = S_get_store_score_gcs_setting(
        local_save_setting=local_save_setting)

    # 存檔至GCS
    gcs.L_upload_to_gcs(gcs_setting=gcs_save_setting)

    # 計算地區的寵物數量
    df_pet_count = T_calculate_pet_count(df=df_pet)

    # 計算category_raw_score
    df_category_score = T_calculate_category_raw_score(
        df_pet_count=df_pet_count, df_main=df_main)

    # 計算norm_city
    city_col_list = ["city_id", "category_id"]
    df_city = T_get_normalize_score(
        df=df_category_score, col_list=city_col_list, col_name="norm_city")

    # 計算norm_all
    all_col_list = ["category_id"]
    df_all = T_get_normalize_score(
        df=df_category_score, col_list=all_col_list, col_name="norm_all")

    # 合併norm_all和norm_city
    df_final = T_merge_city_and_all(df_city=df_city, df_all=df_all)

    # 計算市內和全國的category_score
    df_final = T_calculate_category_score(
        df=df_final, weighted_dict=CATEGORY_WEIGHTED_DICT)

    # 將類別分數按照市內和全國作加總
    df_final = T_calculate_city_all_score(df=df_final)

    # 將市內分數和全國按不同權重後相加計算final score
    df_final = T_calculate_final_score(df=df_final)

    # 將final score還原為0-10分
    df_final = T_transform_final_to_10(
        df=df_final, new_col=CONVENIENCE_SCORE_COLUMNS)

    # 將區域分數資料存回DB
    save_final_db = dfm.L_truncate_and_upload_data_to_db(
        df=df_final, table_name="convenience_score")

    # 取得final地端存檔資訊
    local_final_setting = S_get_convenience_score_save_setting()

    # 存檔至地端
    save_final = dfm.L_save_file_to_csv_by_dict(
        save_setting=local_final_setting, df=df_final)

    # 取得final GCS存檔資訊
    gcs_final_setting = S_get_convenience_score_gcs_setting(
        local_save_setting=local_final_setting)

    # 存檔至GCS
    gcs.L_upload_to_gcs(gcs_setting=gcs_final_setting)

    local_save >> gcs_save_setting, save_final >> gcs_final_setting


d_01_12_convenience_score()
