# L_shelter.py – 儲存 store.csv + 匯入 MySQL

import os
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def get_engine():
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = os.getenv("MYSQL_PORT")
    db_name = os.getenv("MYSQL_DB_NAME")
    return create_engine(f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}")


def save_to_local(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"📦 [L1] 已寫入：{path}")


def save_to_db(df, table):
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table}"))
            df.to_sql(table, con=conn, if_exists="append", index=False)
        print(f"💾 [L2] 匯入 MySQL 完成：{table}")
    except Exception as e:
        print(f"❌ MySQL 匯入失敗：{e}")


def load(df):

    output_path = "/opt/airflow/data/data/complete/store/type=shelter/store.csv"
    table_name = "shelter"

    with ThreadPoolExecutor(max_workers=2) as exe:
        exe.submit(save_to_local, df, output_path)
        exe.submit(save_to_db, df, table_name)
