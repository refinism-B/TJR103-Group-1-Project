import pandas as pd
from pathlib import Path
import os
from datetime import time, date, datetime, timedelta
from airflow.decorators import task

"""
這個模組是關於pandas應用的自訂函式
通常簡寫為 import pandas_mod as pdm
"""

@task
def read_or_build(folder, file, columns):
    """檢查路徑檔案是否存在，若有則讀取，無則建立空表格"""
    file_path = os.path.join(folder, file)
    path = Path(file_path)

    # 若檔案不存在則先新建空的df並存檔
    if path.exists():
        df = pd.read_csv(file_path)
    else:
        df = pd.DataFrame(columns=columns)

    return df, file_path

@task
def exist_or_not(folder, file):
    """檢查路徑檔案是否存在，若有則讀取，無則建立空表格"""
    file_path = os.path.join(folder, file)
    path = Path(file_path)

    return path.exists(), file_path

@task
def reassign_id(df, id_col_name, id_str):
    """根據原有最後一筆資料進行自動延續編號
    對於未編號的資料，需要先建立id欄位並且賦予空字串
    df請輸入想要增加編號的df
    id_col_name請輸入id的「欄位名」
    id_str請輸入編號的「前綴字串」"""

    # 先找出原本的id編號（id欄位非空）最大值
    nums = df.loc[df[id_col_name] != "",
                  id_col_name].str.extract(r"(\d+)").astype(int)

    if nums.empty:
        start_num = 1
    else:
        start_num = nums.max()[0] + 1

    # 計算需要新增的資料數
    empty_id = df[id_col_name] == ""
    empty_id_count = empty_id.sum()

    # 先列出編號list
    new_id = [f"{id_str}{i:04d}" for i in range(
        start_num, start_num + empty_id_count)]

    # 將list放入df欄位
    df.loc[empty_id, id_col_name] = new_id

    return df

@task
def T_combine_dataframe(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    df_combine = pd.concat([df1, df2], ignore_index=True)

    return df_combine


@task
def T_combine_six_dataframe(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df3: pd.DataFrame,
    df4: pd.DataFrame,
    df5: pd.DataFrame,
    df6: pd.DataFrame,
    ) -> pd.DataFrame:

    df_combine = pd.concat([df1, df2, df3, df4, df5, df6], ignore_index=True)

    return df_combine


@task
def T_combine_five_dataframe(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df3: pd.DataFrame,
    df4: pd.DataFrame,
    df5: pd.DataFrame,
    ) -> pd.DataFrame:

    df_combine = pd.concat([df1, df2, df3, df4, df5], ignore_index=True)

    return df_combine




@task
def T_rename_columns(df: pd.DataFrame, col_list: list) -> pd.DataFrame:
    if len(df.columns) != len(col_list):
        raise ValueError("DF欄位數與列表長度不符合！")
    else:
        df.columns = col_list
        return df

@task
def T_drop_columns(df: pd.DataFrame, drop_list: list) -> pd.DataFrame:
    missing_cols = [col for col in drop_list if col not in df.columns]
    if len(missing_cols) != 0:
        raise ValueError("某些欄位不存在df中！")
    else:
        df = df.drop(columns=drop_list, axis=1)
        return df

@task
def T_sort_columns(df: pd.DataFrame, new_cols: list) -> pd.DataFrame:
    missing_cols = [col for col in new_cols if col not in df.columns]
    if len(missing_cols) != 0:
        raise ValueError("某些欄位不存在df中！")
    else:
        df = df[new_cols]
        return df