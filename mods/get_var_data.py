"""
此module包含讀取資料來源的方法
"""

import requests
import pandas as pd
from bs4 import BeautifulSoup


def get_the_html(url: str, headers: dict[str, str]) -> BeautifulSoup:
    """取得網頁原始碼

    Args:
        url (str): 網頁連結
        headers (dict[str, str]): 網頁標頭

    Returns:
        BeautifulSoup: 經過html.parser解析的網頁原始碼，如果有異常會回傳空的BeautifulSoup
    """
    try:
        response = requests.get(url, headers)
        return BeautifulSoup(response.text, "html.parser")
    except requests.exceptions.HTTPError:
        print(f"讀取時發生錯誤，錯誤代碼為{response.status_code}")
        return BeautifulSoup()
    except requests.exceptions.RequestException as err:
        print(f"請求逾時，錯誤代碼{err}")
        return BeautifulSoup()


def get_json_data_no_verify(url: str) -> pd.DataFrame:
    """
    從政府API取得JSON資訊後轉成DataFrame

    Args:
        url (_type_): API網址，必須是JSON格式

    Returns:
        pd.DataFrame: 回傳DataFrame，如果有異常會回傳空的DF
    """
    try:
        response = requests.get(url, verify=False)
        print(f"response_status_code: {response.status_code}")

        # 如果不是200，requests.exceptions.HTTPError
        response.raise_for_status()

        # 將API轉為JSON
        data = response.json()

        # JSON轉成DataFrame
        return pd.DataFrame(data)

    except requests.exceptions.HTTPError:
        print(f"讀取時發生錯誤，錯誤代碼為{response.status_code}")
        return pd.DataFrame()
    except requests.exceptions.RequestException as err:
        print(f"請求逾時，錯誤代碼{err}")
        return pd.DataFrame()


def get_json_data(url: str) -> pd.DataFrame:
    """
    從非政府API取得JSON資訊後轉成DataFrame

    Args:
        url (_type_): API網址，必須是JSON格式

    Returns:
        pd.DataFrame: 回傳DataFrame，如果有異常會回傳空的DF
    """
    try:
        response = requests.get(url)
        print(f"response_status_code: {response.status_code}")

        # 如果不是200，requests.exceptions.HTTPError
        response.raise_for_status()

        # 將API轉為JSON
        data = response.json()

        # JSON轉成DataFrame
        return pd.DataFrame(data)

    except requests.exceptions.HTTPError:
        print(f"讀取時發生錯誤，錯誤代碼為{response.status_code}")
        return pd.DataFrame()
    except requests.exceptions.RequestException as err:
        print(f"請求逾時，錯誤代碼{err}")
        return pd.DataFrame()


def get_csv_data(path: str) -> pd.DataFrame:
    """讀取CSV檔並轉成DataFrame

    Args:
        path (str): CSV檔路徑

    Returns:
        pd.DataFrame: 回傳DataFrame，如果有異常會回傳空的DF
    """
    try:
        # 若遇到編碼問題，可加上 encoding='utf-8' 或 encoding='cp950' 等
        return pd.read_csv(path, encoding="utf-8-sig")
    except FileNotFoundError:
        print(f"檔案不存在: {path}")
    except PermissionError:
        print(f"沒有讀取權限: {path}")
    except IsADirectoryError:
        print(f"指定的是資料夾不是檔案: {path}")
    except pd.errors.EmptyDataError:
        print(f"CSV 檔案為空: {path}")
    except pd.errors.ParserError as e:
        print(f"CSV 解析錯誤: {e}")
    except UnicodeDecodeError as e:
        print(f"編碼錯誤: {e}")
    except OSError as e:
        print(f"I/O 錯誤: {e}")
    except Exception as e:
        # 最後的防護，記錄未知錯誤
        print(f"讀取 CSV 時發生未知錯誤: {e}")
    return pd.DataFrame()
