import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pymysql

# === 載入環境變數 (.env) ===
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

def get_engine():
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    host = os.getenv("MYSQL_IP")
    port = os.getenv("MYSQL_PORT")
    db_name = os.getenv("MYSQL_DB_NAME")

    if not all([username, password, host, port, db_name]):
        raise ValueError("❌ .env 資訊不完整，請確認 MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_IP, MYSQL_PORT, MYSQL_DB_NAME")

    engine = create_engine(f"mysql+pymysql://{username}:{password}@{host}:{port}/{db_name}")
    return engine


def load(df: pd.DataFrame):
    """
    將人口資料寫入 MySQL (population_new)
    """
    print("💾 [L] 開始匯入人口資料到 MySQL...")

    try:
        engine = get_engine()
        table_name = "population_new"

        with engine.begin() as conn:
            # 若表格不存在，自動建立
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.execute(text("COMMIT"))

        print(f"✅ MySQL 匯入成功，共 {len(df)} 筆資料！")

    except Exception as e:
        print(f"❌ MySQL 匯入失敗：{e}")
        raise
