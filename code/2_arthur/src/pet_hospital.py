import time
import os
import numpy as np
import pandas as pd
import googlemaps
from colorama import Fore
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.common import exceptions
from mods import savedata as sd
from mods import extractdata as ed
from mods import gmap as gm
from dotenv import load_dotenv

# 設定Selenium找不到元素與屬性時的錯誤
NoSuchElementException = exceptions.NoSuchElementException
NoSuchAttributeException = exceptions.NoSuchAttributeException

# 讀取.env檔案
load_dotenv()

URL = "https://ahis9.aphia.gov.tw/Veter/OD/HLIndex.aspx"
raw_path = "data/raw/hospital_data.csv"
processed_path = "data/processed/hospital_data_ETL.csv"

# 對edge的options加上headers
options = Options()
options.add_argument("user-agent=MyAgent/1.0")

# 設定edge的driver
driver = webdriver.Edge(options=options)


# TODO: 之後要封裝
def gmap_info(ori_name, api_key, place_id):
    """提供place_id，回傳名稱、營業狀態、營業時間、gmap評分、經緯度、gmap網址、最新評論日期"""
    if pd.notna(place_id) and place_id not in (None, "", "nan"):
        try:
            gmaps = googlemaps.Client(key=api_key)
            detail = gmaps.place(place_id=place_id, language="zh-TW")
        except Exception as e:
            # API 呼叫失敗，回傳 minimal fallback
            return {
                "name": ori_name,
                "place_id": place_id,
                "business_status": None,
                "address": None,
                "phone": None,
                "opening_hours": None,
                "rating": None,
                "rating_total": None,
                "longitude": None,
                "latitude": None,
                "map_url": None,
                "newest_review": None,
            }

        result = detail.get("result") or {}
        name = result.get("name")
        business_status = result.get("business_status")

        formatted_address = result.get("formatted_address")
        adr_address = result.get("adr_address")
        if formatted_address:
            address = formatted_address
        elif adr_address:
            address = BeautifulSoup(adr_address, "html.parser").text
        else:
            address = None

        phone = result.get("formatted_phone_number")
        if isinstance(phone, str):
            phone = phone.replace(" ", "")

        opening_hours = result.get("opening_hours", {}).get("weekday_text")
        rating = result.get("rating")
        rating_total = result.get("user_ratings_total")
        longitude = result.get("geometry", {}).get("location", {}).get("lng")
        latitude = result.get("geometry", {}).get("location", {}).get("lat")
        map_url = result.get("url")
        review_list = result.get("reviews")
        newest_review = gm.newest_review_date(review_list) if review_list else None

        place_info = {
            "name": name,
            "place_id": place_id,
            "business_status": business_status,
            "address": address,
            "phone": phone,
            "opening_hours": opening_hours,
            "rating": rating,
            "rating_total": rating_total,
            "longitude": longitude,
            "latitude": latitude,
            "map_url": map_url,
            "newest_review": newest_review,
        }
    else:
        place_info = {
            "name": ori_name,
            "place_id": None,
            "business_status": None,
            "address": None,
            "phone": None,
            "opening_hours": None,
            "rating": None,
            "rating_total": None,
            "longitude": None,
            "latitude": None,
            "map_url": None,
            "newest_review": None,
        }

    return place_info


def main():
    driver.get(URL)
    time.sleep(2)

    try:
        # 取得識別碼的圖片元素
        captcha_img = driver.find_element(
            By.ID, "ctl00_ContentPlaceHolder1_imgValidateCode"
        )

        # 透過img的src取得驗證碼
        # src的連結中，=後面的數字即為驗證碼
        captcha_src = captcha_img.get_attribute("src").split("=")[-1]
        print(f"驗證碼為: {captcha_src}")

        # 找到文字輸入框並將輸入驗證碼
        driver.find_element(
            By.ID, "ctl00_ContentPlaceHolder1_Field_ValidateCode"
        ).send_keys(captcha_src)

        # 點擊查詢按鈕
        driver.find_element(By.ID, "ctl00_ContentPlaceHolder1_btnSave").click()
    except NoSuchElementException as err:
        print(err)
    except NoSuchAttributeException as err:
        print(err)

    # 延遲三秒讓網頁跑一下
    time.sleep(3)

    # 取得網頁原始碼
    html = driver.page_source

    # 使用bs4解析
    soup = BeautifulSoup(html, "html.parser")

    # 取得class = col-md-12 col-xs-12 的div tag
    div_tag = soup.select("div.col-md-12.col-xs-12 > div.col-md-12")

    # 依序取得div tag裡面的text
    hospital_list = [div.get_text(strip=True) for div in div_tag]

    # 將hospital list中的元素每7個自行組成一個list
    grouped_hospital_list = [
        hospital_list[i : i + 7] for i in range(0, len(hospital_list), 7)
    ]

    # 創建DataFrame
    columns = ["name", "license", "license_date", "vet", "tel", "address", "service"]
    df = pd.DataFrame(data=grouped_hospital_list, columns=columns)
    print("DataFrame已建置完成")

    # 儲存原始CSV檔
    sd.store_to_csv_no_index(df, raw_path)

    # ETL開始
    need_revised_columns = [
        "license",
        "license_date",
        "vet",
        "tel",
        "address",
        "service",
    ]

    # 移除:前面的資料
    for col in need_revised_columns:
        df[col] = (
            df[col]
            .str.split("：")
            .str[-1]
            .str.replace(" ", "", regex=False)
            .str.strip()
        )

    # 將空字串設為NaN
    df = df.replace({"": np.nan})
    df = df.fillna("無此資訊")

    # 執行正則表達比對
    df["city"], df["district"] = zip(*df["address"].apply(ed.extract_city_district))

    # 只取出city非空值的資料，其他drop，所以只會留下六都資訊
    df = df[df["city"].notna()].reset_index(drop=True)

    # drop不需要的欄位
    df = df.drop(columns=["license", "license_date", "vet", "service"])

    # 取得google key
    API_KEY = os.getenv("OGLE_MAP_KEY_CHGWYELLOW")

    # 透過google api並傳送醫院名稱與地址取得醫院的place_id
    result = []

    # enumerate會自動將被iterate的物件附上index
    for i, (idx, row) in enumerate(df.iterrows()):
        query = f"{row['name']} {row['address']}"

        result.append(gm.get_place_id(API_KEY, query))
    df["place_id"] = np.nan
    df.loc[:, "place_id"] = result

    # 透過place_id找到詳細資料
    result = []
    for _, row in df.iterrows():
        result.append(gmap_info(row["name"], API_KEY, row["place_id"]))

    df_google = pd.DataFrame(result)

    df_merged = df.merge(
        df_google,
        how="outer",
        left_on=df["place_id"],
        right_on=df_google["place_id"],
        suffixes=["_filtered", "_checked"],
    )

    # 去除重複欄位
    df_merged = df_merged.drop(columns=["place_id_filtered", "place_id_checked"])

    # 去除重複的place_id
    df_merged = df_merged.drop_duplicates(subset="key_0")

    # 將欄位重新編排
    # 名稱、地址與電話欄位排一起方便比對
    revised_columns = [
        "key_0",
        "name_filtered",
        "name_checked",
        "address_filtered",
        "address_checked",
        "tel",
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

    # 儲存ETL後的資料
    if not df_merged.isna().any():
        sd.store_to_csv_no_index(df_merged, processed_path)
    else:
        print(Fore.RED + "[✗] DataFrame內有空值，請檢查!")


if __name__ == "__main__":
    main()
