from datetime import datetime, timedelta

from airflow.decorators import dag, task
from tasks import database_file_mod as dfm
from tasks import pandas_mod as pdm
from utils.config import GSEARCH_CITY_CODE, STORE_TYPE_ENG_CH_DICT
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
    dag_id="d_01-3_gmap_full_search_restaurant",
    default_args=default_args,
    description="[每月更新]透過經緯度爬取六都「寵物美容」列表",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "monthly", "restaurant", "google_API"]
)
def d_02_2_gmap_full_search_restaurant():

    # 爬取的商店類型，若要修改則在此變更。
    # 0為寵物美容，1為寵物餐廳，2為寵物用品
    keyword_dict = gfs.S_get_keyword_dict(
        dict_name=STORE_TYPE_ENG_CH_DICT, index=1)

    # 將六都及對應的字串名稱取出
    TPE_city_dict = gfs.S_get_city_data(dict_name=GSEARCH_CITY_CODE, index=0)
    TYU_city_dict = gfs.S_get_city_data(dict_name=GSEARCH_CITY_CODE, index=1)
    TCH_city_dict = gfs.S_get_city_data(dict_name=GSEARCH_CITY_CODE, index=2)
    TNA_city_dict = gfs.S_get_city_data(dict_name=GSEARCH_CITY_CODE, index=3)
    KSH_city_dict = gfs.S_get_city_data(dict_name=GSEARCH_CITY_CODE, index=4)

    # 設定搜尋參數：半徑與步長
    search_setting = gfs.S_search_setting(radius=3000, step=3000)

    # 根據搜尋參數設定與六都邊界資料，列出所有座標點
    TPE_loc_points = gfs.S_set_loc_point(
        search_setting=search_setting, city_name=TPE_city_dict["city_name"])
    TYU_loc_points = gfs.S_set_loc_point(
        search_setting=search_setting, city_name=TYU_city_dict["city_name"])
    TCH_loc_points = gfs.S_set_loc_point(
        search_setting=search_setting, city_name=TCH_city_dict["city_name"])
    TNA_loc_points = gfs.S_set_loc_point(
        search_setting=search_setting, city_name=TNA_city_dict["city_name"])
    KSH_loc_points = gfs.S_set_loc_point(
        search_setting=search_setting, city_name=KSH_city_dict["city_name"])

    # 正式使用gmap開始搜尋
    TPE_result_dict = gfs.E_gmap_search(city_data=TPE_city_dict, keyword=keyword_dict,
                                        search_setting=search_setting, loc_points=TPE_loc_points)
    TYU_result_dict = gfs.E_gmap_search(city_data=TYU_city_dict, keyword=keyword_dict,
                                        search_setting=search_setting, loc_points=TYU_loc_points)
    TCH_result_dict = gfs.E_gmap_search(city_data=TCH_city_dict, keyword=keyword_dict,
                                        search_setting=search_setting, loc_points=TCH_loc_points)
    TNA_result_dict = gfs.E_gmap_search(city_data=TNA_city_dict, keyword=keyword_dict,
                                        search_setting=search_setting, loc_points=TNA_loc_points)
    KSH_result_dict = gfs.E_gmap_search(city_data=KSH_city_dict, keyword=keyword_dict,
                                        search_setting=search_setting, loc_points=KSH_loc_points)

    # 將搜尋結果轉換成六都df
    df_TPE = gfs.T_transform_to_df(result_dict=TPE_result_dict)
    df_TYU = gfs.T_transform_to_df(result_dict=TYU_result_dict)
    df_TCH = gfs.T_transform_to_df(result_dict=TCH_result_dict)
    df_TNA = gfs.T_transform_to_df(result_dict=TNA_result_dict)
    df_KSH = gfs.T_transform_to_df(result_dict=KSH_result_dict)

    # 取得六都的存檔設定
    TPE_save_setting = gfs.S_get_save_setting(
        keyword_dict=keyword_dict, city_dict=TPE_city_dict)
    TYU_save_setting = gfs.S_get_save_setting(
        keyword_dict=keyword_dict, city_dict=TYU_city_dict)
    TCH_save_setting = gfs.S_get_save_setting(
        keyword_dict=keyword_dict, city_dict=TCH_city_dict)
    TNA_save_setting = gfs.S_get_save_setting(
        keyword_dict=keyword_dict, city_dict=TNA_city_dict)
    KSH_save_setting = gfs.S_get_save_setting(
        keyword_dict=keyword_dict, city_dict=KSH_city_dict)

    # 將六都df存檔成csv
    dfm.L_save_file_to_csv_by_dict(save_setting=TPE_save_setting, df=df_TPE)
    dfm.L_save_file_to_csv_by_dict(save_setting=TYU_save_setting, df=df_TYU)
    dfm.L_save_file_to_csv_by_dict(save_setting=TCH_save_setting, df=df_TCH)
    dfm.L_save_file_to_csv_by_dict(save_setting=TNA_save_setting, df=df_TNA)
    dfm.L_save_file_to_csv_by_dict(save_setting=KSH_save_setting, df=df_KSH)

    # 取得gmap的metadata紀錄並轉成df
    df_meta_TPE = gfs.T_transform_metadata_df(result_dict=TPE_result_dict)
    df_meta_TYU = gfs.T_transform_metadata_df(result_dict=TYU_result_dict)
    df_meta_TCH = gfs.T_transform_metadata_df(result_dict=TCH_result_dict)
    df_meta_TNA = gfs.T_transform_metadata_df(result_dict=TNA_result_dict)
    df_meta_KSH = gfs.T_transform_metadata_df(result_dict=KSH_result_dict)

    # 將metadata的df合併
    df_metadata = pdm.T_combine_five_dataframe(
        df1=df_meta_TPE,
        df2=df_meta_TYU,
        df3=df_meta_TCH,
        df4=df_meta_TNA,
        df5=df_meta_KSH)

    # 取得metadata存檔資訊
    metadata_save_setting = gfs.S_get_metadata_save_setting()

    # 將metadata的紀錄存檔成csv
    dfm.L_save_file_to_csv_by_dict(
        save_setting=metadata_save_setting, df=df_metadata)

    # 簡單清理檔案
    # 去除place id重複資料
    df_TPE = gfs.T_drop_duplicated(df=df_TPE)
    df_TYU = gfs.T_drop_duplicated(df=df_TYU)
    df_TCH = gfs.T_drop_duplicated(df=df_TCH)
    df_TNA = gfs.T_drop_duplicated(df=df_TNA)
    df_KSH = gfs.T_drop_duplicated(df=df_KSH)

    # 去除非正常營業資料
    df_TPE = gfs.T_keep_operation_store(df=df_TPE)
    df_TYU = gfs.T_keep_operation_store(df=df_TYU)
    df_TCH = gfs.T_keep_operation_store(df=df_TCH)
    df_TNA = gfs.T_keep_operation_store(df=df_TNA)
    df_KSH = gfs.T_keep_operation_store(df=df_KSH)

    # 去除沒有地理資料的店家
    df_TPE = gfs.T_drop_no_geometry(df=df_TPE)
    df_TYU = gfs.T_drop_no_geometry(df=df_TYU)
    df_TCH = gfs.T_drop_no_geometry(df=df_TCH)
    df_TNA = gfs.T_drop_no_geometry(df=df_TNA)
    df_KSH = gfs.T_drop_no_geometry(df=df_KSH)

    # 根據地理資訊查詢是否真的在六都邊界內
    df_TPE = gfs.T_detect_in_boundary_or_not(
        df=df_TPE, city_dict=TPE_city_dict)
    df_TYU = gfs.T_detect_in_boundary_or_not(
        df=df_TYU, city_dict=TYU_city_dict)
    df_TCH = gfs.T_detect_in_boundary_or_not(
        df=df_TCH, city_dict=TCH_city_dict)
    df_TNA = gfs.T_detect_in_boundary_or_not(
        df=df_TNA, city_dict=TNA_city_dict)
    df_KSH = gfs.T_detect_in_boundary_or_not(
        df=df_KSH, city_dict=KSH_city_dict)

    # 去除不在邊界內的資料
    df_TPE = gfs.T_drop_data_out_boundary(df=df_TPE)
    df_TYU = gfs.T_drop_data_out_boundary(df=df_TYU)
    df_TCH = gfs.T_drop_data_out_boundary(df=df_TCH)
    df_TNA = gfs.T_drop_data_out_boundary(df=df_TNA)
    df_KSH = gfs.T_drop_data_out_boundary(df=df_KSH)

    # 將五個df合併
    df_main = pdm.T_combine_five_dataframe(
        df1=df_TPE,
        df2=df_TYU,
        df3=df_TCH,
        df4=df_TNA,
        df5=df_KSH
    )

    # 合併後再次去除重複place id資料
    df_main = gfs.T_drop_duplicated(df=df_main)

    # 新增更新日期
    df_main = gfs.T_add_update_date(df=df_main)

    # 取得存檔設定
    main_save_setting = gfs.S_get_main_save_setting(keyword_dict=keyword_dict)

    # 存檔至地端
    dfm.L_save_file_to_csv_by_dict(save_setting=main_save_setting, df=df_main)


d_02_2_gmap_full_search_restaurant()
