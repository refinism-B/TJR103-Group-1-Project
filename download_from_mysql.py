import os
from pathlib import Path

import pandas as pd
import pymysql
from dotenv import load_dotenv


def create_pymysql_connect():
    """
    自動透過pymysql建立連線，回傳conn連線物件。
    所需各項資料請寫入.env檔案中。請勿直接寫於程式中。
    """

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


def E_load_from_sql(table_name: str) -> pd.DataFrame:
    """
    輸入欲查詢的表名table_name，透過pymysql連線資料庫，
    並取得該表後將其轉成dataframe。

    連線所需資訊請寫入.env中，請勿寫入程式中。
    """

    conn = create_pymysql_connect()
    sql = f"SELECT * FROM {table_name}"

    try:
        df = pd.read_sql(sql, conn)
        return df.to_dict(orient='records')

    except Exception as e:
        raise Exception(f"讀取{table_name}表時發生錯誤：{e}")


data_type = "restaurant"

data_loc = E_load_from_sql(table_name=data_type)
df_loc = pd.DataFrame(data_loc)

folder = Path(r"C:\Users\add41\Documents\Data_Engineer\Project\example_data")
file_name = f"{data_type}.csv"
path = folder / file_name

df_loc.to_csv(path, index=False, encoding="utf-8-sig")
