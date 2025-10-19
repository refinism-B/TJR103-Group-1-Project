"""
此module包含儲存成CSV的方法
Creator: Chgwyellow
"""

import os
import pandas as pd


def store_to_csv_no_index(df: pd.DataFrame, path: str):
    """將DataFrame儲存成CSV檔且不含index

    Args:
        df (pd.DataFrame): DataFrame
        path (str): 儲存路徑，如果該路徑不存在會自動建立
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print("不含index的CSV檔已存檔完畢")


def store_to_csv(df: pd.DataFrame, path: str):
    """將DataFrame儲存成CSV檔

    Args:
        df (pd.DataFrame): DataFrame
        path (str): 儲存路徑，如果該路徑不存在會自動建立
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path)
    print("CSV檔已存檔完畢")


def store_to_csv_no_index_no_header(df: pd.DataFrame, path: str):
    """將DataFrame儲存成CSV檔且沒有index跟header

    Args:
        df (pd.DataFrame): DataFrame
        path (str): 儲存路徑，如果該路徑不存在會自動建立
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, header=False)
    print("不含index和header的CSV檔已存檔完畢")


def store_to_csv_no_header(df: pd.DataFrame, path: str):
    """將DataFrame儲存成CSV檔且沒有header

    Args:
        df (pd.DataFrame): DataFrame
        path (str): 儲存路徑，如果該路徑不存在會自動建立
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, header=False)
    print("不含header的CSV檔已存檔完畢")
