# L_shelter.py
import os
import pandas as pd
import pymysql
import math
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

def get_conn():
    return pymysql.connect(
        host=os.getenv("MYSQL_IP"),
        user=os.getenv("MYSQL_USERNAME"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB_NAME"),
        port=int(os.getenv("MYSQL_PORT")),
        charset="utf8mb4"
    )

def convert_nan_to_none(row):
    """逐欄位把 NaN / NaT / nan-like 全部轉成 None"""
    new_row = []
    for v in row:
        if v is None:
            new_row.append(None)
        elif isinstance(v, float) and math.isnan(v):
            new_row.append(None)
        elif v == "nan" or v == "NaN":
            new_row.append(None)
        else:
            new_row.append(v)
    return new_row

def load(df, table="shelter"):
    print("💾 [L] Load - 匯入 MySQL 中...")

    # Airflow CSV
    output_path = "/opt/airflow/data/data/complete/store/type=shelter/store.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"📦 [L1] 已寫入：{output_path}")

    # 轉掉所有 NaN：逐列處理（最保險）
    rows = [convert_nan_to_none(row) for row in df.values.tolist()]

    conn = get_conn()
    cursor = conn.cursor()

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    try:
        cursor.executemany(sql, rows)
        conn.commit()
        print("✅ MySQL 匯入成功！（executemany + 完整 NaN 處理）")

    except Exception as e:
        print(f"❌ MySQL 匯入失敗：{e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
