"""
各種提取資料做轉化的函式庫
Creator: Chgwyellow

from mods import extractdata as ed
"""

import re
import pandas as pd
import numpy as np
from mods import gmap as gm
from mods import connectDB as connDB


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


def clean_google_data(
    df: pd.DataFrame,
    api_key: str,
    host: str,
    port: int,
    user: str,
    password: str,
    db: str,
) -> pd.DataFrame:
    # 透過google api並傳送名稱與地址取得place_id
    result = []

    # enumerate會自動將被iterate的物件附上index
    for i, (idx, row) in enumerate(df.iterrows()):
        query = f"{row['name']} {row['address']}"
        result.append(gm.get_place_id(api_key, query))
    # 先創建一個欄位後再填入資料
    df["place_id"] = np.nan
    df.loc[:, "place_id"] = result

    # drop place_id為空的資料
    df_filtered = df.dropna(subset="place_id")

    # 透過place_id找到詳細資料
    result = []
    for _, row in df_filtered.iterrows():
        result.append(gm.gmap_info(row["name"], api_key, row["place_id"]))
    df_checked = pd.DataFrame(result)

    # drop place_id為nan的值
    df_checked = df_checked.dropna(subset="place_id")

    # 將原始的df和經過google api取得資料的df做合併
    df_merged = df_filtered.merge(
        df_checked,
        how="outer",
        left_on=df_filtered["place_id"],
        right_on=df_checked["place_id"],
        suffixes=["_filtered", "_checked"],
    )

    # 只留下business_status為OPERATIONAL的資料
    df_merged = df_merged[df_merged["business_status"] == "OPERATIONAL"]

    # 去除重複欄位
    df_merged = df_merged.drop(columns=["place_id_filtered", "place_id_checked"])

    # 修改columns順序
    revised_columns = [
        "key_0",
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
    df_merged = df_merged[revised_columns]

    # drop重複的key_0
    df_merged = df_merged.drop_duplicates(subset="key_0")

    # 需要計算的欄位空值補0
    fillna_columns = ["opening_hours", "rating", "rating_total", "newest_review"]
    df_merged[fillna_columns] = df_merged[fillna_columns].fillna(0)

    # 連線DB
    conn, cursor = connDB.connect_db(host, port, user, password, db)

    # 讀取location表格的資料並轉成DataFrame
    df_loc = connDB.get_loc_table(conn, cursor)

    # merge df_merged和df_loc
    df_final = df_merged.merge(
        df_loc, left_on="district", right_on="district", how="left"
    )
    df_final = df_final.drop(columns=["city_y"])
    df_final = df_final.rename(columns={"city_x": "city"})

    # 重新編排columns順序
    columns = [
        "key_0",
        "name_checked",
        "address_checked",
        "phone",
        "city",
        "district",
        "loc_id",
        "business_status",
        "opening_hours",
        "rating",
        "rating_total",
        "longitude",
        "latitude",
        "map_url",
        "newest_review",
    ]
    df_final = df_final[columns]

    cursor.close()
    conn.close()

    return df_final
