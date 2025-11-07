import pandas as pd
from pathlib import Path
from datetime import date, datetime, timedelta
import os
from dotenv import load_dotenv
from airflow.decorators import task
from sqlalchemy import create_engine
import pymysql


def create_pymysql_connect():
    load_dotenv()

    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = int(os.getenv("MYSQL_PORTT"))
    db_name = os.getenv("MYSQL_DB_NAME")

    conn = pymysql.connect(
        host=target_ip,
        port=target_port,
        user=username,
        password=password,
        database=db_name,
        charset='utf8mb4'
    )

    return conn


@task
def L_save_file_to_csv_by_dict(save_setting: dict, df: pd.DataFrame):
    try:
        folder = Path(save_setting["folder"])
        folder.mkdir(parents=True, exist_ok=True)
        file_name = save_setting["file_name"]
        path = folder / file_name
        df.to_csv(path, index=False, encoding="utf-8-sig")

        print(f"{file_name}地端存檔成功！")
    except Exception as e:
        print({f"{file_name}地端存檔失敗：{e}"})


@task
def L_save_file_to_csv(folder: str, file_name: str, df: pd.DataFrame):
    try:
        folder = Path(folder)
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / file_name
        df.to_csv(path, index=False, encoding="utf-8-sig")

        print(f"{file_name}地端存檔成功！")

    except Exception as e:
        print({f"{file_name}地端存檔失敗：{e}"})


@task
def E_load_file_from_csv(folder: str, file_name: str) -> pd.DataFrame:
    folder = Path(folder)
    path = folder / file_name
    cols = pd.read_csv(path, nrows=0).columns

    dtype_dict = {col: str for col in cols if col.lower() == 'phone'}
    df = pd.read_csv(path, dtype=dtype_dict)

    return df


@task
def E_load_file_from_csv_by_dict(read_setting: dict) -> pd.DataFrame:
    folder = Path(read_setting["folder"])
    file_name = read_setting["file_name"]
    path = folder / file_name
    cols = pd.read_csv(path, nrows=0).columns

    dtype_dict = {col: str for col in cols if col.lower() == 'phone'}
    df = pd.read_csv(path, dtype=dtype_dict)

    return df


# @task
# def L_save_to_sql(df: pd.DataFrame, table_name: str, save_mod: str) -> tuple[bool, str]:
#     conn = create_pymysql_connect()
#     try:
#         df.to_sql(name=table_name,
#                   con=engine, index=False, if_exists=save_mod)
#         print(f"{table_name}已輸入資料庫成功！")

#     except Exception as e:
#         print({f"{table_name}輸入資料庫失敗：{e}"})


@task
def E_load_from_sql(table_name: str) -> pd.DataFrame:
    conn = create_pymysql_connect()
    sql = f"SELECT * FROM {table_name}"

    try:
        df = pd.read_sql(sql, conn)
        return df.to_dict(orient='records')

    except Exception as e:
        raise Exception(f"讀取{table_name}表時發生錯誤：{e}")
