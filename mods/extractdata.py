"""
各種提取資料做轉化的函式庫
Creator: Chgwyellow

from mods import extractdata as ed
"""

import ast
import re
import pandas as pd
import numpy as np
from mods import gmap as gm
from mods import connectDB as connDB
from mods import savedata as sd
from mods import date_mod as dm
from colorama import Fore


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


def gdata_etl(
    df: pd.DataFrame,
    api_key: str,
    host: str,
    port: int,
    user: str,
    password: str,
    db: str,
    id_sign: str,
    save_path: str,
) -> pd.DataFrame:
    """
    清理與補充 Google 寵物旅館資料
    使用前請確認傳入的df欄位只有name, address, city和district
    ------------------------------------------------------------
    1. 透過 Google API 補上 place_id 與詳細資訊
    2. 篩選出營業中的商家 (business_status == "OPERATIONAL")
    3. 整理欄位順序、補空值、產生 hotel_id
    4. 與 location 表格合併 loc_id
    """

    # ------------------------------------------------------------
    # 取得 place_id
    # ------------------------------------------------------------
    place_ids = []
    for _, row in df.iterrows():
        query = f"{row['name']} {row['address']}"
        place_ids.append(gm.get_place_id(api_key, query))
    # 未避免place_id長度與df不同，先創建欄位後再填入資料
    df["place_id"] = np.nan
    df.loc[:, "place_id"] = place_ids
    df_filtered = df.dropna(subset="place_id")
    print(Fore.GREEN + "✅ place_id has been found.")

    # ------------------------------------------------------------
    # 取得 Google Maps 詳細資料
    # ------------------------------------------------------------
    detailed_results = [
        gm.gmap_info(row["name"], api_key, row["place_id"])
        for _, row in df_filtered.iterrows()
    ]
    df_checked = pd.DataFrame(detailed_results).dropna(subset=["place_id"])
    print(Fore.GREEN + "✅ Google details have been found.")

    # ------------------------------------------------------------
    # 合併原始資料與 Google API 詳細資料
    # ------------------------------------------------------------
    df_merged = df_filtered.merge(
        df_checked,
        on="place_id",
        how="outer",
        suffixes=("_filtered", "_checked"),
    )

    # 保留營業中的旅館
    df_merged = df_merged[df_merged["business_status"] == "OPERATIONAL"]
    print(Fore.GREEN + "✅ Successfully merged the original data with the google data.")

    # ------------------------------------------------------------
    # 清理與整理欄位
    # ------------------------------------------------------------

    # 如果有 key_0，改回 place_id
    if "key_0" in df_merged.columns and "place_id" not in df_merged.columns:
        df_merged.rename(columns={"key_0": "place_id"}, inplace=True)

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
        "newest_review",
    ]
    df_merged = df_merged[revised_columns].drop_duplicates(subset=["place_id"])

    # 填補空值
    fillna_columns = ["opening_hours", "rating", "rating_total", "newest_review"]
    df_merged[fillna_columns] = df_merged[fillna_columns].fillna(0)
    print(Fore.GREEN + "✅ Columns have been sorted and fill the missing value.")

    # ------------------------------------------------------------
    # 從資料庫讀取 location 表格
    # ------------------------------------------------------------

    # 連線DB
    conn, cursor = connDB.connect_db(host, port, user, password, db)
    df_loc = connDB.get_loc_table(conn, cursor)
    cursor.close()
    conn.close()
    print(Fore.GREEN + "✅ Cursor and connection have been closed.")

    # ------------------------------------------------------------
    # 與 location 表格合併 (加入 loc_id)
    # ------------------------------------------------------------
    df_final = df_merged.merge(
        df_loc, left_on=["city", "district"], right_on=["city", "district"], how="left"
    )
    df_final = df_final.sort_values(["city", "district"])
    print(Fore.GREEN + "✅ Location table has been merged with the original data.")

    # ------------------------------------------------------------
    # 產生 id（例如：ht0001, ht0002...）
    # ------------------------------------------------------------
    df_final["id"] = np.nan
    num_hotel_id = df_final["id"].isna().sum()
    new_ids = [f"{id_sign}{str(i).zfill(4)}" for i in range(1, num_hotel_id + 1)]
    df_final.loc[:, "id"] = new_ids
    print(Fore.GREEN + "✅ id column has been serialized.")

    # ------------------------------------------------------------
    # 修改opening_time欄位
    # ------------------------------------------------------------
    df_final["opening_hours"] = df["opening_hours"].apply(dm.trans_op_time_to_hours)

    # ------------------------------------------------------------
    # 調整欄位順序與名稱
    # ------------------------------------------------------------
    final_columns = [
        "hotel_id",
        "place_id",
        "name_checked",
        "address_checked",
        "phone",
        "city",
        "district",
        "loc_id",
        "business_status",
        "opening_hours",
        "types",
        "rating",
        "rating_total",
        "longitude",
        "latitude",
        "map_url",
        "website",
        "newest_review",
    ]
    df_final = df_final[final_columns]
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
