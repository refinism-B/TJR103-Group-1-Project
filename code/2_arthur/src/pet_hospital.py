import time
import os
import pandas as pd
import googlemaps
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


if __name__ == "__main__":
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

    # 初步ETL
    # 只保留name和address
    need_columns = ["name", "address"]
    df = df[need_columns]

    # 移除:前面的資料
    df["address"] = (
        df["address"]
        .str.split("：")
        .str[-1]
        .str.replace(" ", "", regex=False)
        .str.strip()
    )

    # 執行正則表達比對並建立city, district欄位
    df["city"], df["district"] = zip(*df["address"].apply(ed.extract_city_district))

    # 只取出city非空值的資料，其他drop，所以只會留下六都資訊
    df = df[df["city"].notna()].reset_index(drop=True)

    # ------------------------------------------------------------

    # 執行合併ETL
    host = os.getenv("MYSQL_IP")
    port = int(os.getenv("MYSQL_PORTT"))
    user = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    db = os.getenv("MYSQL_DB_NAME")
    id_sign = "hp"
    API_KEY = os.getenv("GOOGLE_MAP_KEY_CHGWYELLOW")

    df_final = ed.gdata_etl(
        df,
        API_KEY,
        host,
        port,
        user,
        password,
        db,
        id_sign=id_sign,
        save_path=processed_path,
    )
