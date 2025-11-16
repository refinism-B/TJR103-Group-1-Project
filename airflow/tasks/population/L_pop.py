# L_pop.py
import os
import pandas as pd
import pymysql
import math
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

# === MySQL 主連線（for CREATE/TRUNCATE）===
def get_engine():
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = os.getenv("MYSQL_PORT")
    db_name = os.getenv("MYSQL_DB_NAME")
    return create_engine(
        f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}",
        future=True,
    )

# === PyMySQL（for executemany insert）===
def get_conn():
    return pymysql.connect(
        host=os.getenv("MYSQL_IP"),
        user=os.getenv("MYSQL_USERNAME"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB_NAME"),
        port=int(os.getenv("MYSQL_PORT")),
        charset="utf8mb4"
    )

# 單列 NaN → None 轉換
def convert_nan_to_none(row):
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

# === 檔案輸出目錄 ===
BASE_DIR = os.path.join(os.getcwd(), "airflow", "data")
RAW_DIR = os.path.join(BASE_DIR, "raw", "population")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed", "population")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

RAW_PATH = os.path.join(RAW_DIR, "population_raw.csv")
PROCESSED_PATH = os.path.join(PROCESSED_DIR, "population_processed.csv")


# === 主 Load 函式 ===
def load(df: pd.DataFrame):
    print("💾 [L] Load Population - 開始匯出與匯入...")

    # === 輸出 CSV（給 Airflow or Debug）===
    print(f"📦 RAW 輸出至：{RAW_PATH}")
    df.to_csv(RAW_PATH, index=False, encoding="utf-8-sig")

    print(f"📦 Processed 輸出至：{PROCESSED_PATH}")
    df.to_csv(PROCESSED_PATH, index=False, encoding="utf-8-sig")

    # === 準備寫入 population_new ===
    engine = get_engine()

    try:
        with engine.begin() as conn:
            print("🗄️ 建立 population_new / 清空...")
            conn.execute(text("CREATE TABLE IF NOT EXISTS population_new LIKE population;"))
            conn.execute(text("TRUNCATE TABLE population_new;"))
    except Exception as e:
        print(f"❌ 建立/清空 population_new 失敗: {e}")
        return

    # === DataFrame → Python list rows（逐值轉 None）===
    rows = [convert_nan_to_none(row) for row in df.values.tolist()]

    # === executemany 寫入 population_new（最穩的方法）===
    conn = get_conn()
    cursor = conn.cursor()

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO population_new ({cols}) VALUES ({placeholders})"

    try:
        print("🚀 寫入 MySQL population_new 中...")
        cursor.executemany(sql, rows)
        conn.commit()
        print("✅ population_new 匯入成功！")

    except Exception as e:
        conn.rollback()
        print(f"❌ 寫入 population_new 時發生錯誤：{e}")

    finally:
        cursor.close()
        conn.close()

    print("🎉 Population ETL Load 完成！")
