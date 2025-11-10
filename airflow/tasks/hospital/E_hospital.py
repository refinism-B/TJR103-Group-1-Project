import time
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.edge.options import Options
from selenium.common import exceptions
from selenium.webdriver.common.by import By
from utils import savedata as sd


def main():
    # 設定Selenium找不到元素與屬性時的錯誤
    NoSuchElementException = exceptions.NoSuchElementException
    NoSuchAttributeException = exceptions.NoSuchAttributeException

    URL = "https://ahis9.aphia.gov.tw/Veter/OD/HLIndex.aspx"
    raw_path = "/opt/airflow/data/raw/hospital/hospital_data.csv"

    # 對edge的options加上headers
    options = Options()
    options.add_argument("user-agent=MyAgent/1.0")

    # 設定edge的driver
    driver = webdriver.Edge(options=options)

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


if __name__ == "__main__":
    main()
