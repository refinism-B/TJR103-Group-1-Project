"""
各種提取資料做轉化的函式庫
Creator: Chgwyellow

from mods import extractdata as ed
"""

import ast
import re

import numpy as np
import pandas as pd
from colorama import Fore
from utils import connectDB as connDB
from utils import date_mod as dm
from utils import gmap as gm
from utils import savedata as sd


def extract_city_district(address: str) -> tuple[str, str]:
    """從機構的地址取出所在市與區
    此處re的pattern是設定為六都及轄下區域

    Args:
        address (str): 要尋找的地址

    Returns:
        tuple[str, str]: 前者返回city, 後者返回district, 如果沒有則都返回None
    """

    # 這裡pattern用六都的方式做設定
    pattern = r"(臺北市|台北市|新北市|桃園市|台中市|臺中市|台南市|臺南市|高雄市)(.*?區)"
    match = re.search(pattern=pattern, string=address)
    if match:
        return match.group(1), match.group(2)
    return None, None


def extract_city_district_from_df(df: pd.DataFrame, address_col: str) -> pd.DataFrame:
    """從 DataFrame 的地址欄位提取城市和地區資訊

    Args:
        df (pd.DataFrame): 要處理的 DataFrame
        address_col (str): 地址欄位名稱

    Returns:
        pd.DataFrame: 添加 city 和 district 欄位後的 DataFrame
    """
    # 使用 zip 和 apply 提取城市與地區
    df["city"], df["district"] = zip(*df[address_col].apply(extract_city_district))

    # 只保留有城市資訊的資料列(六都)
    df = df[df["city"].notna()].reset_index(drop=True)

    return df


def gdata_place_id(df: pd.DataFrame, api_key: str, save_path: str) -> pd.DataFrame:
    """
    清理與補充 Google 寵物旅館資料
    使用前請確認傳入的df欄位只有name, address, city和district
    ------------------------------------------------------------
    1. 透過 Google API 補上 place_id
    """
    # ------------------------------------------------------------
    # 取得 place_id
    # ------------------------------------------------------------
    print(api_key)
    place_ids = []
    for _, row in df.iterrows():
        query = f"{row['name']} {row['address']}"
        print(Fore.GREEN + f"✅ place_id{_} has been found.")
        place_ids.append(gm.get_place_id(api_key, query))
    # 未避免place_id長度與df不同，先創建欄位後再填入資料
    df["place_id"] = np.nan
    df.loc[:, "place_id"] = place_ids
    df_filtered = df.dropna(subset="place_id")
    # 儲存google爬下來含有place_id的檔案
    sd.store_to_csv_no_index(df_filtered, save_path)
    return df_filtered


def gdata_info(df: pd.DataFrame, api_key: str, save_path: str):
    """
    1. 透過 Google API 補上詳細資訊
    2. 篩選出營業中的商家 (business_status == "OPERATIONAL")
    3. 整理欄位順序、補空值並儲存資料
    """
    # ------------------------------------------------------------
    # 取得 Google Maps 詳細資料
    # ------------------------------------------------------------
    detailed_results = [
        gm.gmap_info(row["name"], api_key, row["place_id"]) for _, row in df.iterrows()
    ]
    df_checked = pd.DataFrame(detailed_results).dropna(subset=["place_id"])
    print(Fore.GREEN + "✅ Google details have been found.")

    # ------------------------------------------------------------
    # 合併原始資料與 Google API 詳細資料
    # ------------------------------------------------------------
    df_merged = df.merge(
        df_checked,
        on="place_id",
        how="outer",
        suffixes=("_filtered", "_checked"),
    )
    # 儲存google爬下來含有詳細資料的檔案
    sd.store_to_csv_no_index(df_merged, save_path)
    return df_merged


def clean_sort(df: pd.DataFrame, save_path: str):
    """business_status為營業中，整理欄位名稱與補空值

    Args:
        df (pd.DataFrame): 取的google詳細資料的df
        save_path (str): 儲存路徑

    Returns:
        _type_: 整理後的df
    """
    # 保留營業中的旅館
    df_merged = df[df["business_status"] == "OPERATIONAL"]
    print(Fore.GREEN + "✅ Successfully merged the original data with the google data.")

    # ------------------------------------------------------------
    # 清理與整理欄位
    # ------------------------------------------------------------

    # 如果有 key_0，改回 place_id
    if "key_0" in df_merged.columns and "place_id" not in df_merged.columns:
        df_merged.rename(columns={"key_0": "place_id"}, inplace=True)

    # 填補空值
    fillna_columns = ["opening_hours", "rating", "rating_total"]
    df_merged[fillna_columns] = df_merged[fillna_columns].fillna(0)
    print(Fore.GREEN + "✅ Columns have been sorted and fill the missing value.")

    # 修改columns順序
    revised_columns = [
        "place_id",
        "name_checked",
        "address_checked",
        "phone",
        "city",
        "district",
        "business_status",
        "opening_hours",
        "rating",
        "rating_total",
        "longitude",
        "latitude",
        "map_url",
        "website",
        "newest_review",
    ]
    df_merged = df_merged[revised_columns].drop_duplicates(subset=["place_id"])

    # ------------------------------------------------------------
    # 修改opening_hours欄位
    # ------------------------------------------------------------
    # csv讀進來時list會被轉成字串，所以先將str轉成list
    # df_merged["opening_hours"] = df_merged["opening_hours"].apply(str_to_list)
    df_merged.loc[:, "opening_hours"] = df_merged["opening_hours"].apply(
        dm.trans_op_time_to_hours
    )

    # ------------------------------------------------------------
    # 轉換pd空值
    # ------------------------------------------------------------
    for col in df_merged.columns:
        df_merged[col] = df_merged[col].apply(to_sql_null)

    # # types欄位解開list
    # df_merged.loc[:, "types"] = df_merged["types"].apply(
    #     lambda x: ",".join(x) if isinstance(x, list) else ""
    # )

    # 儲存修改後的檔案
    sd.store_to_csv_no_index(df_merged, save_path)

    return df_merged


def merge_loc(
    df: pd.DataFrame,
    host: str,
    port: int,
    user: str,
    password: str,
    db: str,
    save_path: str,
) -> pd.DataFrame:
    """讀取DB中的location表並合併

    Args:
        df (pd.DataFrame): 經過google data合併的df
        host (str): 主機名稱
        port (int): port號
        user (str): 使用者名稱
        password (str): 使用者密碼
        db (str): 資料庫名稱
        save_path (str): 儲存路徑
    Returns:
        pd.DataFrame: 合併location後的df
    """
    # ------------------------------------------------------------
    # 從資料庫讀取 location 表格
    # ------------------------------------------------------------

    # 連線DB
    conn, cursor = connDB.connect_db(host, port, user, password, db)
    df_loc = connDB.get_loc_table(conn, cursor)
    # cursor.close()
    # conn.close()
    print(Fore.GREEN + "✅ Cursor and connection have been closed.")

    # ------------------------------------------------------------
    # 與 location 表格合併 (加入 loc_id)
    # ------------------------------------------------------------
    df_final = df.merge(
        df_loc, left_on=["city", "district"], right_on=["city", "district"], how="inner"
    )
    # 依照city和district排序
    df_final = df_final.sort_values(["city", "district"])
    print(Fore.GREEN + "✅ Location table has been merged with the original data.")

    # 儲存修改後的檔案
    sd.store_to_csv_no_index(df_final, save_path)
    return df_final


def create_id(df: pd.DataFrame, id_sign: str, save_path: str) -> pd.DataFrame:
    """產生id號碼

    Args:
        df (pd.DataFrame): 經過location表格合併的df
        id_sign (str): id開頭文字
        save_path (str): 儲存路徑

    Returns:
        pd.DataFrame: 含有id欄位的df
    """
    # ------------------------------------------------------------
    # 產生 id（例如：ht0001, ht0002...）
    # ------------------------------------------------------------
    # 統一台->臺
    df["city"] = df["city"].str.replace("台", "臺")
    # 排序
    df = df.sort_values(["city", "district"])
    df["id"] = np.nan
    num_id = df["id"].isna().sum()
    new_ids = [f"{id_sign}{str(i).zfill(4)}" for i in range(1, num_id + 1)]
    df.loc[:, "id"] = new_ids
    print(Fore.GREEN + "✅ id column has been serialized.")
    sd.store_to_csv_no_index(df, save_path)

    return df


def cat_id(
    df: pd.DataFrame,
    host: str,
    port: int,
    user: str,
    password: str,
    db: str,
    save_path: str,
    category: str,
) -> pd.DataFrame:
    # 連線DB
    conn, cursor = connDB.connect_db(host, port, user, password, db)
    # 讀取category表格的資料
    sql = f"""
    select category_id
    from category
    where category_eng = '{category}';
    """
    cursor.execute(sql)
    cat = cursor.fetchall()
    cursor.close()
    conn.close()
    print(Fore.GREEN + "✅ Cursor and connection have been closed.")

    # 轉成df
    df_cat = pd.DataFrame(data=cat, columns=["category_id"])

    # 原本的df創立一個cat_id並賦值
    df["cat_id"] = df_cat["category_id"].iloc[0]

    if category == "hospital":
        # 判斷168小時醫院並將cat_id變成7
        df.loc[df["opening_hours"] == 168, "cat_id"] = 7

    # 調整欄位
    columns = [
        "id",
        "place_id",
        "name_checked",
        "address_checked",
        "phone",
        "city",
        "district",
        "loc_id",
        "business_status",
        "opening_hours",
        "cat_id",
        "rating",
        "rating_total",
        "longitude",
        "latitude",
        "map_url",
        "website",
        "newest_review",
    ]
    df = df[columns]
    sd.store_to_csv_no_index(df, save_path)

    return df


def to_sql_data(df: pd.DataFrame, save_path: str):
    """調整欄位順序與儲存最終檔案

    Args:
        df (pd.DataFrame): 含有id的df，請確認opening_hour欄位已改成數字且空值已轉換
        save_path (str): 儲存路徑

    Returns:
        _type_: 可以寫入DB的df
    """
    # ------------------------------------------------------------
    # 調整欄位順序與名稱
    # ------------------------------------------------------------
    final_columns = [
        "id",
        "name_checked",
        "business_status",
        "loc_id",
        "address_checked",
        "phone",
        "opening_hours",
        "cat_id",
        "rating",
        "rating_total",
        "newest_review",
        "longitude",
        "latitude",
        "map_url",
        "website",
        "place_id",
    ]
    df_final = df[final_columns]
    print(Fore.GREEN + "✅ Final table has finished.")

    # ------------------------------------------------------------
    # 將最終結果儲存成csv檔
    # ------------------------------------------------------------
    sd.store_to_csv_no_index(df_final, save_path)

    return df_final


def str_to_list(x: str) -> list:
    """csv檔讀取list進來時會自動變成string，此函式可以將字串轉為list

    Args:
        x (str): 字串型態資料

    Returns:
        list: 經過處理後的list
    """
    try:
        val = ast.literal_eval(x)

        # ✅ 如果轉完是 list 且裡面第一個元素也是 list
        # 表示外面多包了一層，要取第一層的內容
        if isinstance(val, list) and len(val) == 1 and isinstance(val[0], list):
            return val[0]

        # ✅ 正常 list
        if isinstance(val, list):
            return val

        # 不是 list，就包成 list
        return [val]

    except (ValueError, SyntaxError):
        return [x]


def fix_phone(x):
    if x is None or pd.isna(x):
        return None

    # 若是 float，先用 int 去掉小數
    if isinstance(x, float):
        x = str(int(x))

    s = str(x).strip()
    s = re.sub(r"\D", "", s)  # 留數字

    if s == "":
        return None

    # 若長度是 9 位，但本來應該是 10 位手機
    # 代表 0 被吃掉 → 補回去
    if len(s) == 9:
        s = "0" + s

    # 手機格式
    if len(s) == 10 and s.startswith("09"):
        return s

    # 市話
    if len(s) in (9, 10) and s.startswith("0"):
        return s

    return None


def to_sql_null(x):
    """將pandas的空值轉為Python的None

    Args:
        x (_type_): 傳入的欄位值

    Returns:
        None: 就是None
    """
    if pd.isna(x):
        return None
    s = str(x).strip()

    if s.lower() in ("nan", "none", ""):
        return None
    return x
