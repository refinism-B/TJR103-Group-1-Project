import os
import time
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.common import exceptions

# 設定Selenium找不到元素與屬性時的錯誤
NoSuchElementException = exceptions.NoSuchElementException
NoSuchAttributeException = exceptions.NoSuchAttributeException

URL = "https://ahis9.aphia.gov.tw/Veter/OD/HLIndex.aspx"
raw_path = "../../data/raw/hospital_data.csv"
processed_path = "../../data/processed/hospital_data_ETL.csv"

# 對edge的options加上headers
options = Options()
options.add_argument("user-agent=MyAgent/1.0")

# 設定edge的driver
driver = webdriver.Edge(options=options)


def get_into_webpage_html(url: str, driver: webdriver) -> BeautifulSoup:
    """透過edge driver進入網頁抓取原始碼

    Args:
        url (str): 目標網址
        driver (webdriver): edge driver

    Returns:
        BeautifulSoup: 解析後的html碼
    """
    driver.get(url)
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
    return BeautifulSoup(html, "html.parser")


def get_hospital_data_save(soup: BeautifulSoup, raw_path: str) -> pd.DataFrame:
    """取得醫院相關資料，轉成DataFrame後儲存

    Args:
        soup (BeautifulSoup): 解析後的html碼
        raw_path (str): 原始檔案儲存位置

    Returns:
        pd.DataFrame: 資料表
    """
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

    # 儲存為csv檔
    # 若目錄不存在則建立
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    df.to_csv(raw_path, index=False)
    print("原始CSV檔已儲存完畢")

    return df


def data_process(df: pd.DataFrame, processed_path: str):
    """資料清理並儲存檔案

    Args:
        df (pd.DataFrame): 資料表
        processed_path (str): 處理後資料的儲存路徑
    """
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
        df[col] = df[col].str.split("：").str[-1].str.strip()

    # 將空字串設為NaN
    df = df.replace({"": np.nan})
    df = df.fillna("無此資訊")

    # 儲存為整理後的csv
    os.makedirs(os.path.dirname(processed_path), exist_ok=True)
    df.to_csv(processed_path, index=False)
    print("ETL後的CSV檔已儲存完畢")


def main():
    soup = get_into_webpage_html(url=URL, driver=driver)
    df = get_hospital_data_save(soup, raw_path)
    data_process(df, processed_path)


if __name__ == "__main__":
    main()
