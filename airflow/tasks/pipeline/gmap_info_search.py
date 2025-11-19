import os
import re
import time
from datetime import date
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from dotenv import load_dotenv
from opencc import OpenCC
from tasks import date_mod as dtm
from utils import gmap_mod as gm
from utils.config import (ADDRESS_DROP_KEYWORDS,
                          GMAP_INFO_SEARCH_FINAL_COLUMNS, STORE_DROP_KEY_WORDS,
                          STORE_TYPE_CODE_DICT, STORE_TYPE_ENG_CH_DICT,
                          WORDS_REPLACE_FROM_ADDRESS)


@task
def S_get_keyword_dict(dict_name: dict, index: int) -> dict:
    """
    設定要處理的店家類型，
    0為寵物美容，1為寵物餐廳，2為寵物用品。
    """

    keyword_ch_list = list(dict_name.keys())
    keyword_list = list(dict_name.values())

    return {"keyword": keyword_ch_list[index], "file_name": keyword_list[index]}


@task
def S_get_read_setting(keyword_dict: dict) -> dict:
    """設定讀取檔案的路徑資訊"""
    folder = f"/opt/airflow/data/processed/{keyword_dict['file_name']}"
    file_name = f"{keyword_dict['file_name']}_place_id.csv"

    return {"folder": folder, "file_name": file_name}


@task
def S_get_read_setting_small(keyword_dict: dict) -> dict:
    """設定讀取檔案的路徑資訊"""
    folder = f"/opt/airflow/data/test/{keyword_dict['file_name']}"
    file_name = f"{keyword_dict['file_name']}_place_id_small.csv"

    return {"folder": folder, "file_name": file_name}


@task
def S_get_temp_save_setting(keyword_dict: dict) -> dict:
    """設定迴圈中的臨時存檔資訊"""
    folder = f"/opt/airflow/data/raw/{keyword_dict['file_name']}/info"
    file_name = f"{keyword_dict['file_name']}_temp.csv"

    return {"folder": folder, "file_name": file_name}


@task
def S_get_temp_save_setting_small(keyword_dict: dict) -> dict:
    """設定迴圈中的臨時存檔資訊"""
    folder = f"/opt/airflow/data/test/{keyword_dict['file_name']}/info"
    file_name = f"{keyword_dict['file_name']}_temp_small.csv"

    return {"folder": folder, "file_name": file_name}


@task
def E_get_store_info_by_gmap(df: pd.DataFrame, temp_save_setting: dict) -> pd.DataFrame:
    """
    根據dataframe中的place id進行gmap詳細資料搜尋，
    並在每次搜尋時都進行臨時存檔。
    """

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
    """僅保留需要的欄位資料"""
    cols = ['place_id', 'address', 'phone', 'opening_hours', 'rating',
            'rating_total', 'longitude', 'latitude', 'map_url', 'website', 'newest_review']
    df = df[cols]

    return df


@task
def T_merge_df(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """將兩個df進行merge，並根據place id欄位合併"""
    df = df1.merge(df2, how="left", on="place_id")

    return df


@task
def S_get_raw_info_save_setting(keyword_dict: dict) -> dict:
    """設定爬取的原始資料的存檔資訊"""
    folder = f"/opt/airflow/data/raw/{keyword_dict['file_name']}/info"
    file_name = f"{keyword_dict['file_name']}_info_raw.csv"

    return {"folder": folder, "file_name": file_name}


@task
def S_get_raw_info_save_setting_small(keyword_dict: dict) -> dict:
    """設定爬取的原始資料的存檔資訊"""
    folder = f"/opt/airflow/data/testw/{keyword_dict['file_name']}/info"
    file_name = f"{keyword_dict['file_name']}_info_raw_small.csv"

    return {"folder": folder, "file_name": file_name}


@task
def T_drop_data_by_store_name(df: pd.DataFrame, keyword_list: list) -> pd.DataFrame:
    """去除店家名稱包含某些關鍵字（通常表示歇業或沒有提供服務）的資料"""
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
    """
    去除"in_boundary", "update_date", "geometry"三個欄位，
    並去除地址為空值的資料。
    """

    drop_cols = ["in_boundary", "update_date", "geometry"]
    df = df.drop(columns=drop_cols, axis=1)
    df = df.dropna(subset=["address"])

    return df


@task
def T_add_category_and_fillna(df: pd.DataFrame, keyword_dict: dict) -> pd.DataFrame:
    """增加類別欄位，並將評論數、評分為空值的補0"""
    df["category"] = keyword_dict["keyword"]

    df[["rating", "rating_total"]] = df[[
        "rating", "rating_total"]].fillna(0)

    return df


@task
def T_clean_address(df: pd.DataFrame, replace_dict: dict) -> pd.DataFrame:
    """
    清理地址欄位：先做簡轉繁確保都是繁體、並替換一些錯別字。
    並新增市及區欄位（先填入空值）。
    """
    cc = OpenCC("s2t")
    df["address"] = df["address"].apply(cc.convert)

    for word in replace_dict:
        df["address"] = df["address"].str.replace(word, replace_dict[word])

    df["city"] = None
    df["district"] = None

    return df


@task
def T_first_address_format(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
    """抓取市、區資料。第一種情況：正常地址格式"""
    mask1 = df["address"].str.contains(pattern, regex=True, na=False)
    extracted1 = df.loc[mask1, "address"].str.extract(pattern)
    df.loc[mask1, ["city", "district"]] = extracted1.values

    return df


@task
def T_second_address_format(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
    """抓取市、區資料。第二種情況：倒反地址格式"""
    mask3 = df["address"].str.contains(pattern, regex=True, na=False)
    extracted3 = df.loc[mask3, "address"].str.extract(pattern)

    df.loc[mask3, "city"] = extracted3[1].values
    df.loc[mask3, "district"] = extracted3[0].values

    return df


@task
def T_third_address_format(df: pd.DataFrame, pattern: str) -> pd.DataFrame:
    """抓取市、區資料。第三種情況：只有區沒有市"""
    mask2 = df["address"].str.contains(pattern, regex=True, na=False)
    extracted2 = df.loc[mask2, "address"].str.extract(pattern)
    df.loc[mask2, "district"] = extracted2[0].values

    return df


@task
def T_clean_district_word(df: pd.DataFrame, drop_word_list: list) -> pd.DataFrame:
    """清理"區"的資料，將不乾淨的字元去除"""
    df["district"] = df["district"].str.replace("路竹", "鹿竹")
    pattern4 = "|".join(map(re.escape, drop_word_list))
    df["district"] = df["district"].str.replace(pattern4, "", regex=True)
    df["district"] = df["district"].str.replace("鹿竹", "路竹")

    return df


@task
def T_df_merge_location(df_main: pd.DataFrame, df_loc: pd.DataFrame) -> pd.DataFrame:
    """與location資料merge，並只保留loc id"""
    df_loc = pd.DataFrame(df_loc)
    df_loc = df_loc[["loc_id", "city", "district"]]
    print(df_loc.head())
    print("-----------------------")
    print(df_main["city"])
    print("-----------------------")
    print(df_main["district"])
    print("-----------------------")
    print(f"df_main總筆數：{len(df_main)}")
    print("-----------------------")

    # 第一次merge
    df_main = df_main.merge(df_loc, how="left", on=["city", "district"])
    print(df_main["loc_id"])

    # 取出loc id為空值，沒有市資料的索引
    miss_loc = df_main["loc_id"].isna()
    print(f"缺失loc_id的資料數：{len(miss_loc)}")

    # 如果有則進行二次join
    if len(miss_loc) != 0:
        df_miss = df_main[miss_loc].drop(columns="loc_id")
        df_miss = df_miss.merge(df_loc, how="left", on="district")
        df_main.loc[miss_loc, "loc_id"] = df_miss["loc_id"].values

    df_main = df_main.drop(columns=["city", "district"])

    return df_main


@task
def T_df_merge_category(df_main: pd.DataFrame, df_category: pd.DataFrame) -> pd.DataFrame:
    """與category資料merge，並指保留category id"""
    df_category = pd.DataFrame(df_category)
    df_main = df_main.merge(df_category, how="left",
                            left_on="category", right_on="category_name")
    df_main = df_main.drop(
        columns=["category_name", "category_eng", "category"])

    return df_main


@task
def S_get_id_columns_setting(type_dict: dict, keyword_dict: dict) -> pd.DataFrame:
    """取得id欄位的設定dict"""
    store_type_name = keyword_dict["keyword"]
    id_str = type_dict[store_type_name]

    return {"id_cols": "id", "id_str": id_str}


@task
def T_add_id_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """將dataframe建立空字串的id欄位"""
    df["id"] = ""

    return df


@task
def T_trans_op_time_to_hours(df: pd.DataFrame) -> pd.DataFrame:
    """將營業時間資料轉換成營業的時數（以週為單位）"""
    op_hours_list = []
    for index, row in df.iterrows():
        op_time = row["opening_hours"]
        op_hours = dtm.trans_op_time_to_hours(op_time=op_time)
        op_hours_list.append(op_hours)

    df["op_hours"] = op_hours_list
    df = df.drop(columns="opening_hours")

    return df


@task
def S_get_finish_save_setting(keyword_dict: dict) -> dict:
    """取得完成檔的存檔資訊"""
    file_date = date.today().strftime("%Y%m%d")
    folder = f"/opt/airflow/data/complete/store/type={keyword_dict['file_name']}"
    file_name = "store.csv"

    return {"folder": folder, "file_name": file_name}


@task
def S_get_finish_save_setting_small(keyword_dict: dict) -> dict:
    """取得完成檔的存檔資訊"""
    file_date = date.today().strftime("%Y%m%d")
    folder = f"/opt/airflow/data/test/store/type={keyword_dict['file_name']}"
    file_name = "store_small.csv"

    return {"folder": folder, "file_name": file_name}


@task
def S_get_gcs_setting(keyword_dict: dict, local_save_setting: dict) -> dict:
    """取得上傳GCS的設定資訊"""
    folder = Path(local_save_setting["folder"])
    file_name = local_save_setting["file_name"]
    path = str(folder / file_name)
    keyword = keyword_dict['file_name']

    bucket_name = "tjr103-1-project-bucket"
    destination = f"data/complete/store/type={keyword}/store.csv"
    source_file_name = path

    return {
        "bucket_name": bucket_name,
        "destination": destination,
        "source_file_name": source_file_name
    }


@task
def S_get_gcs_setting_small(keyword_dict: dict, local_save_setting: dict) -> dict:
    """取得上傳GCS的設定資訊"""
    folder = Path(local_save_setting["folder"])
    file_name = local_save_setting["file_name"]
    path = str(folder / file_name)
    keyword = keyword_dict['file_name']

    bucket_name = "tjr103-1-project-bucket"
    destination = f"test_data/complete/store/type={keyword}/store.csv"
    source_file_name = path

    return {
        "bucket_name": bucket_name,
        "destination": destination,
        "source_file_name": source_file_name
    }


@task
def S_print_result(ori_count: int, finish_count: int):
    """將資料清理前後的筆數相比，顯示清理的結果如何"""
    clean_count = ori_count - finish_count
    print(f"清理前資料筆數：{ori_count}")
    print(f"清理後資料筆數：{finish_count}")
    print(f"清理掉的資料筆數：{clean_count}")
