import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from dotenv import load_dotenv
from opencc import OpenCC
from tasks import database_file_mod as dfm
from tasks import date_mod as dtm
from tasks import pandas_mod as pdm
from utils import gmap_mod as gm
from utils.config import (ADDRESS_DROP_KEYWORDS, STORE_DROP_KEY_WORDS,
                          STORE_TYPE_CODE_DICT, STORE_TYPE_ENG_CH_DICT,
                          WORDS_REPLACE_FROM_ADDRESS, GMAP_INFO_SEARCH_FINAL_COLUMNS)

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
    dag_id="d_03_1_gmap_info_search_salon",
    default_args=default_args,
    description="[每月更新][寵物美容]根據place id資料爬取店家詳細資料",
    schedule_interval="0 */2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "monthly", "salon", "test_done"]
)
def d_03_1_gmap_info_search_salon():

    def S_get_keyword_dict(dict_name: dict, index: int) -> dict:
        keyword_ch_list = list(dict_name.keys())
        keyword_list = list(dict_name.values())

        return {"keyword": keyword_ch_list[index], "file_name": keyword_list[index]}

    def S_get_read_setting(keyword_dict: dict) -> dict:
        folder = f"/opt/airflow/data/processed/{keyword_dict['file_name']}"
        file_name = f"{keyword_dict['file_name']}_place_id.csv"

        return {"folder": folder, "file_name": file_name}

    def S_get_temp_save_setting(keyword_dict: dict):
        folder = f"/opt/airflow/data/raw/{keyword_dict['file_name']}/info"
        file_name = f"{keyword_dict['file_name']}_temp.csv"

        return {"folder": folder, "file_name": file_name}

    @task
    def E_get_store_info_by_gmap(df: pd.DataFrame, temp_save_setting: dict) -> pd.DataFrame:
        load_dotenv()
        api_key = os.getenv("GMAP_KEY6")
        place_id_list = df["place_id"].values

        data = []
        count = 1
        total = len(place_id_list)
        for id_ in place_id_list:
            print(f"開始搜尋第{count}/{total}筆資料...")
            place_info = gm.get_place_dict(api_key=api_key, place_id=id_)
            data.append(place_info)
            df_temp = pd.DataFrame(data=data)

            temp_folder = Path(temp_save_setting["folder"])
            temp_folder.mkdir(parents=True, exist_ok=True)
            temp_file_name = temp_save_setting["file_name"]
            temp_path = temp_folder / temp_file_name
            df_temp.to_csv(temp_path, index=False, encoding="utf-8-sig")

            count += 1
            time.sleep(1)

        df = pd.DataFrame(data=data)
        print(f"已完成全部{total}筆資料抓取")

        return df

    @task
    def T_keep_needed_columns(df: pd.DataFrame) -> pd.DataFrame:
        cols = ['place_id', 'address', 'phone', 'opening_hours', 'rating',
                'rating_total', 'longitude', 'latitude', 'map_url', 'website', 'newest_review']
        df = df[cols]

        return df

    @task
    def T_merge_df(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
        df = df1.merge(df2, how="left", on="place_id")

        return df

    def S_get_raw_info_save_setting(keyword_dict: dict) -> dict:
        folder = f"/opt/airflow/data/raw/{keyword_dict['file_name']}/info"
        file_name = f"{keyword_dict['file_name']}_info_raw.csv"

        return {"folder": folder, "file_name": file_name}\


    @task
    def T_drop_data_by_store_name(df: pd.DataFrame, keyword_list: list) -> pd.DataFrame:
        del_idx = []

        for keyword in keyword_list:
            idx = df[df["name"].str.contains(keyword, na=False)].index
            if len(idx) != 0:
                del_idx.extend(idx)
        del_idx = list(set(del_idx))

        df = df.drop(index=del_idx, axis=0)

        return df

    @task
    def T_drop_cols_and_na(df: pd.DataFrame) -> pd.DataFrame:
        drop_cols = ["in_boundary", "update_date", "geometry"]
        df = df.drop(columns=drop_cols, axis=1)
        df = df.dropna(subset=["address"])

        return df

    @task
    def T_add_category_and_fillna(df: pd.DataFrame, keyword_dict: dict) -> pd.DataFrame:
        df["category"] = keyword_dict["keyword"]

        df[["rating", "rating_total"]] = df[[
            "rating", "rating_total"]].fillna(0)

        return df

    @task
    def T_clean_address(df: pd.DataFrame, replace_dict: dict) -> pd.DataFrame:
        cc = OpenCC("s2t")
        df["address"] = df["address"].apply(cc.convert)

        for word in replace_dict:
            df["address"] = df["address"].str.replace(word, replace_dict[word])

        df["city"] = None
        df["district"] = None

        return df

    @task
    def T_first_address_format(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
        mask1 = df["address"].str.contains(pattern, regex=True, na=False)
        extracted1 = df.loc[mask1, "address"].str.extract(pattern)
        df.loc[mask1, ["city", "district"]] = extracted1.values

        return df

    @task
    def T_second_address_format(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
        mask3 = df["address"].str.contains(pattern, regex=True, na=False)
        extracted3 = df.loc[mask3, "address"].str.extract(pattern)

        df.loc[mask3, "city"] = extracted3[1].values
        df.loc[mask3, "district"] = extracted3[0].values

        return df

    @task
    def T_third_address_format(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
        mask2 = df["address"].str.contains(pattern, regex=True, na=False)
        extracted2 = df.loc[mask2, "address"].str.extract(pattern)
        df.loc[mask2, "district"] = extracted2[0].values

        return df

    @task
    def T_clean_district_word(df: pd.DataFrame, drop_word_list: list) -> pd.DataFrame:
        df["district"] = df["district"].str.replace("路竹", "鹿竹")
        pattern4 = "|".join(map(re.escape, drop_word_list))
        df["district"] = df["district"].str.replace(pattern4, "", regex=True)
        df["district"] = df["district"].str.replace("鹿竹", "路竹")

        return df

    @task
    def T_df_merge_location(df_main: pd.DataFrame, df_loc: pd.DataFrame) -> pd.DataFrame:
        df_loc = pd.DataFrame(df_loc)
        df_loc = df_loc[["loc_id", "city", "district"]]

        # 第一次merge
        df_main = df_main.merge(df_loc, how="left", on=["city", "district"])

        # 取出loc id為空值，沒有市資料的索引
        miss_loc = df_main["loc_id"].isna()

        # 如果有則進行二次join
        if len(miss_loc) != 0:
            df_miss = df_main[miss_loc].drop(columns="loc_id")
            df_miss = df_miss.merge(df_loc, how="left", on="district")
            df_main.loc[miss_loc, "loc_id"] = df_miss["loc_id"].values

        df_main = df_main.drop(columns=["city", "district"])

        return df_main

    @task
    def T_df_merge_category(df_main: pd.DataFrame, df_category: pd.DataFrame) -> pd.DataFrame:
        df_category = pd.DataFrame(df_category)
        df_main = df_main.merge(df_category, how="left",
                                left_on="category", right_on="category_name")
        df_main = df_main.drop(
            columns=["category_name", "category_eng", "category"])

        return df_main

    @task
    def S_get_id_columns_setting(type_dict: dict, keyword_dict: dict) -> pd.DataFrame:
        store_type_name = keyword_dict["keyword"]
        id_str = type_dict[store_type_name]

        return {"id_cols": "id", "id_str": id_str}

    @task
    def T_add_id_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
        df["id"] = ""

        return df

    @task
    def T_trans_op_time_to_hours(df: pd.DataFrame):
        op_hours_list = []
        for index, row in df.iterrows():
            op_time = row["opening_hours"]
            op_hours = dtm.trans_op_time_to_hours(op_time=op_time)
            op_hours_list.append(op_hours)

        df["op_hours"] = op_hours_list
        df = df.drop(columns="opening_hours")

        return df

    def S_get_finish_save_setting(keyword_dict: dict) -> dict:
        file_date = date.today().strftime("%Y%m%d")
        folder = f"/opt/airflow/data/complete/{keyword_dict['file_name']}/dt={file_date}"
        file_name = f"{keyword_dict['file_name']}_finish.csv"

        return {"folder": folder, "file_name": file_name}

    @task
    def S_print_result(ori_count: int, finish_count: int)
    clean_count = ori_count - finish_count

    print(f"清理前資料筆數：{origin_data_total}")
    print(f"清理後資料筆數：{finish_data_total}")
    print(f"清理掉的資料筆數：{clean_count}")
#
#
#
#
#
    # 先定義要執行的商店類別
    # 0為寵物美容、1為寵物餐廳、2為寵物用品
    keyword_dict = S_get_keyword_dict(
        dict_name=STORE_TYPE_ENG_CH_DICT, index=0)

    # 設定讀取路徑及檔案名
    read_setting = S_get_read_setting(keyword_dict=keyword_dict)

    # 將place id表讀入
    df_place = dfm.E_load_file_from_csv_by_dict(read_setting=read_setting)

    # 取得暫存檔存檔設定
    temp_save_setting = S_get_temp_save_setting(keyword_dict=keyword_dict)

    # 開始gmap搜尋商店資訊
    df_search = E_get_store_info_by_gmap(
        df=df_place, temp_save_setting=temp_save_setting)

    # 保留需要的欄位
    df_search = T_keep_needed_columns(df=df_search)

    # 與原本place id表結合
    df_main = T_merge_df(df1=df_place, df2=df_search)

    # 取得存檔設定
    raw_save_setting = S_get_raw_info_save_setting(keyword_dict=keyword_dict)

    # 先將爬取的raw data存檔
    dfm.L_save_file_to_csv_by_dict(save_setting=raw_save_setting, df=df_main)

    # 開始資料清洗
    # 紀錄原資料筆數
    origin_data_total = pdm.S_count_data(df=df_main)

    # 先去除店家標題隱含關閉的資料
    df_main = T_drop_data_by_store_name(
        df=df_main, keyword_list=STORE_DROP_KEY_WORDS)

    # 處理掉部分不需要的欄位及地址為空的資料
    df_main = T_drop_cols_and_na(df=df_main)

    # 新增類別欄位並補空值
    df_main = T_add_category_and_fillna(df=df_main, keyword_dict=keyword_dict)

    # 清理地址中的錯別字或簡繁轉換
    df_main = T_clean_address(
        df=df_main, replace_dict=WORDS_REPLACE_FROM_ADDRESS)

    # 開始透過正則表達式擷取市和區資訊
    # 先定義正則化規則
    pattern1 = r"([^\d\s]{2}市)([^\d\s]{1,3}區)"
    pattern2 = r"灣([^\d\s]{1,2}區)"
    pattern3 = r"([^\d\s]{2}區)([^\d\s]{2}市)"

    # 第一種類型：正常地址格式
    df_main = T_first_address_format(df=df_main, pattern=pattern1)

    # 第二種類型：倒反地址格式
    df_main = T_second_address_format(df=df_main, pattern=pattern3)

    # 第三種類型：只有區沒有市
    df_main = T_third_address_format(df=df_main, pattern=pattern2)

    # 清理區資料中不乾淨的字元
    df_main = T_clean_district_word(
        df=df_main, drop_word_list=ADDRESS_DROP_KEYWORDS)

    # 取得資料庫中的location表
    df_loc = dfm.E_load_from_sql(table_name="location")

    # 與location表合併取得loc id
    df_main = T_df_merge_location(df_main=df_main, df_loc=df_loc)

    # 取得資料庫中的category表
    df_category = dfm.E_load_from_sql(table_name="category")

    # 與category表合併取得category id
    df_main = T_df_merge_category(df_main=df_main, df_category=df_category)

    # 建立空的id欄位
    df_main = T_add_id_empty_columns(df=df_main)

    # 取得id欄位資訊
    id_setting = S_get_id_columns_setting(
        type_dict=STORE_TYPE_CODE_DICT, keyword_dict=keyword_dict)

    # 輸入id欄位值
    df_main = pdm.T_reassign_id(df=df_main, setting_dict=id_setting)

    # 轉換營業時間為時數
    df_main = T_trans_op_time_to_hours(df=df_main)

    # 將欄位重新排序
    df_main = pdm.T_sort_columns(
        df=df_main, new_cols=GMAP_INFO_SEARCH_FINAL_COLUMNS)

    # 設定存檔訊息
    finish_save_setting = S_get_finish_save_setting(keyword_dict=keyword_dict)

    # 存檔至地端
    dfm.L_save_file_to_csv_by_dict(
        save_setting=finish_save_setting, df=df_main)

    # 查看完成後的資料筆數
    finish_data_total = pdm.S_count_data(df_main)

    # 印出比較結果
    S_print_result(ori_count=origin_data_total, finish_count=finish_data_total)


d_03_1_gmap_info_search_salon()
