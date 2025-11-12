from datetime import datetime, timedelta

from airflow.decorators import dag, task
from tasks import database_file_mod as dfm
from tasks import pandas_mod as pdm
from tasks import GCS_mod as gcs
from tasks.pipeline import gmap_info_search as gis
from utils.config import (ADDRESS_DROP_KEYWORDS,
                          GMAP_INFO_SEARCH_FINAL_COLUMNS, STORE_DROP_KEY_WORDS,
                          STORE_TYPE_CODE_DICT, STORE_TYPE_ENG_CH_DICT,
                          WORDS_REPLACE_FROM_ADDRESS)

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
    dag_id="d_01-7_gmap_info_search_supplies_test",
    default_args=default_args,
    description="[每月更新][寵物用品]根據place id資料爬取店家詳細資料",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "monthly", "supplies", "google_API"]
)
def d_01_7_gmap_info_search_supplies():

    # 先定義要執行的商店類別
    # 0為寵物美容、1為寵物餐廳、2為寵物用品
    keyword_dict = gis.S_get_keyword_dict(
        dict_name=STORE_TYPE_ENG_CH_DICT, index=2)

    # 設定讀取路徑及檔案名
    read_setting = gis.S_get_read_setting(keyword_dict=keyword_dict)

    # 將place id表讀入
    df_place = dfm.E_load_file_from_csv_by_dict(read_setting=read_setting)

    # 取得暫存檔存檔設定
    temp_save_setting = gis.S_get_temp_save_setting(keyword_dict=keyword_dict)

    # 開始gmap搜尋商店資訊
    df_search = gis.E_get_store_info_by_gmap(
        df=df_place, temp_save_setting=temp_save_setting)

    # 保留需要的欄位
    df_search = gis.T_keep_needed_columns(df=df_search)

    # 與原本place id表結合
    df_main = gis.T_merge_df(df1=df_place, df2=df_search)

    # 取得存檔設定
    raw_save_setting = gis.S_get_raw_info_save_setting(
        keyword_dict=keyword_dict)

    # 先將爬取的raw data存檔
    dfm.L_save_file_to_csv_by_dict(save_setting=raw_save_setting, df=df_main)

    # 開始資料清洗
    # 紀錄原資料筆數
    origin_data_total = pdm.S_count_data(df=df_main)

    # 先去除店家標題隱含關閉的資料
    df_main = gis.T_drop_data_by_store_name(
        df=df_main, keyword_list=STORE_DROP_KEY_WORDS)

    # 處理掉部分不需要的欄位及地址為空的資料
    df_main = gis.T_drop_cols_and_na(df=df_main)

    # 新增類別欄位並補空值
    df_main = gis.T_add_category_and_fillna(
        df=df_main, keyword_dict=keyword_dict)

    # 清理地址中的錯別字或簡繁轉換
    df_main = gis.T_clean_address(
        df=df_main, replace_dict=WORDS_REPLACE_FROM_ADDRESS)

    # 開始透過正則表達式擷取市和區資訊
    # 先定義正則化規則
    pattern1 = r"([^\d\s]{2}市)([^\d\s]{1,3}區)"
    pattern2 = r"灣([^\d\s]{1,2}區)"
    pattern3 = r"([^\d\s]{2}區)([^\d\s]{2}市)"

    # 第一種類型：正常地址格式
    df_main = gis.T_first_address_format(df=df_main, pattern=pattern1)

    # 第二種類型：倒反地址格式
    df_main = gis.T_second_address_format(df=df_main, pattern=pattern3)

    # 第三種類型：只有區沒有市
    df_main = gis.T_third_address_format(df=df_main, pattern=pattern2)

    # 清理區資料中不乾淨的字元
    df_main = gis.T_clean_district_word(
        df=df_main, drop_word_list=ADDRESS_DROP_KEYWORDS)

    # 取得資料庫中的location表
    df_loc = dfm.E_load_from_sql(table_name="location")

    # 與location表合併取得loc id
    df_main = gis.T_df_merge_location(df_main=df_main, df_loc=df_loc)

    # 取得資料庫中的category表
    df_category = dfm.E_load_from_sql(table_name="category")

    # 與category表合併取得category id
    df_main = gis.T_df_merge_category(df_main=df_main, df_category=df_category)

    # 建立空的id欄位
    df_main = gis.T_add_id_empty_columns(df=df_main)

    # 取得id欄位資訊
    id_setting = gis.S_get_id_columns_setting(
        type_dict=STORE_TYPE_CODE_DICT, keyword_dict=keyword_dict)

    # 輸入id欄位值
    df_main = pdm.T_reassign_id(df=df_main, setting_dict=id_setting)

    # 轉換營業時間為時數
    df_main = gis.T_trans_op_time_to_hours(df=df_main)

    # 將欄位重新排序
    df_main = pdm.T_sort_columns(
        df=df_main, new_cols=GMAP_INFO_SEARCH_FINAL_COLUMNS)

    # 設定存檔訊息
    finish_save_setting = gis.S_get_finish_save_setting(
        keyword_dict=keyword_dict)

    # 存檔至地端
    dfm.L_save_file_to_csv_by_dict(
        save_setting=finish_save_setting, df=df_main)

    # 取得上傳GCS設定檔
    """
    目前設定路徑為test_data，正式時請改成正式路徑
    """
    gcs_setting = gis.S_get_gcs_setting(
        keyword_dict=keyword_dict, local_save_setting=finish_save_setting)

    # 上傳至GCS
    gcs.L_upload_to_gcs(gcs_setting=gcs_setting)

    # 查看完成後的資料筆數
    finish_data_total = pdm.S_count_data(df_main)

    # 印出比較結果
    gis.S_print_result(ori_count=origin_data_total,
                       finish_count=finish_data_total)


d_01_7_gmap_info_search_supplies()
