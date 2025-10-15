import pandas as pd
import os
import requests
from bs4 import BeautifulSoup

# 設定URL和headers
URL = "https://www.dogcatstar.com/blog/24hour-animal-hospital/?srsltid=AfmBOorT4sCf-7J95E1GuTg29XR3w-xYXl2qm-PZL1eOjPJ0k3ndzEem"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
}
raw_path = "../../data/raw/hospital_24_hr_data.csv"
processed_path = "../../data/processed/hospital_24_hr_data_ETL.csv"


def get_the_html(url: str, headers: dict[str, str]) -> BeautifulSoup:
    """取得網頁原始碼

    Args:
        url (str): 網頁連結
        headers (dict[str, str]): 網頁標頭

    Returns:
        BeautifulSoup: 經過html.parser解析的網頁原始碼
    """
    response = requests.get(url, headers)
    soup = BeautifulSoup(response.text, "html.parser")
    return soup


def get_name_address(soup: BeautifulSoup) -> tuple[list, list]:
    """取得醫院的名稱與地址

    Args:
        soup (BeautifulSoup): 解析過的網頁原始碼

    Returns:
        tuple[list, list]: 回傳醫院的名稱與地址
    """
    # 所有的資訊都在p tag裡面
    p_tags = soup.select("p")
    p_texts = [p.get_text(strip=True) for p in p_tags]

    # 只擷取符合醫院的elements
    p_texts = p_texts[3:9] + p_texts[13:51]

    # 只留下名稱跟地址的資料
    hospital_name_address = [item.split("電話：")[0] for item in p_texts]

    # 將名稱與地址資料個別儲存成兩個list
    hospital_name = [name.split("地址：")[0].strip() for name in hospital_name_address]
    hospital_address = [
        address.split("地址：")[-1].strip() for address in hospital_name_address
    ]

    return hospital_name, hospital_address


def to_dataframe_save(name: list, address: list, raw_path: str) -> pd.DataFrame:
    """將名稱與地址轉換成DataFrame並儲存原始的CSV

    Args:
        name (list): 醫院名稱
        address (list): 醫院地址
        raw_path (str): 儲存原始CSV的路徑

    Returns:
        pd.DataFrame: 由名稱與地址組成的資料表
    """
    # 將醫院名稱與地址放入DataFrame
    df = pd.DataFrame({"hospital_name": name, "hospital_address": address})

    # 儲存row data
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    df.to_csv(raw_path, index=False)
    print("原始CSV檔已儲存完畢")

    return df


def data_process(df: pd.DataFrame, processed_path: str):
    """執行資料ETL

    Args:
        df (pd.DataFrame): 原始DataFrame
        processed_path (str): ETL後的儲存路徑
    """
    # 執行ETL
    df["hospital_name"] = (
        df["hospital_name"].str.split("名稱：").str[-1].str.strip()
    )

    # 儲存ETL後的CSV檔
    os.makedirs(os.path.dirname(processed_path), exist_ok=True)
    df.to_csv(processed_path, index=False)
    print("ETL後的CSV檔已儲存完畢")


def main():
    soup = get_the_html(url=URL, headers=headers)
    hospital_name, hospital_address = get_name_address(soup=soup)
    df = to_dataframe_save(hospital_name, hospital_address, raw_path)
    data_process(df=df, processed_path=processed_path)


if __name__ == "__main__":
    main()
