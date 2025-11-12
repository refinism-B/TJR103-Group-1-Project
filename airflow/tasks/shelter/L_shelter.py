import os
from sqlalchemy import create_engine, text
import pandas as pd
from dotenv import load_dotenv
import traceback

load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

def get_engine():
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = os.getenv("MYSQL_PORT")
    db_name = os.getenv("MYSQL_DB_NAME")

    if not all([username, password, target_ip, target_port, db_name]):
        raise ValueError("❌ .env 資訊不完整，請確認 MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_IP, MYSQL_PORT, MYSQL_DB_NAME")

    return create_engine(f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}")

def load(df):
    print("💾 [L] Load - 匯入 MySQL 中...")
    engine = get_engine()

    try:
        with engine.begin() as conn:
            print("🧹 清空舊資料表...")
            conn.execute(text("TRUNCATE TABLE shelter"))
            print("📤 匯入新資料中...")
            df.to_sql("shelter", con=conn, if_exists="append", index=False)
        print("✅ MySQL 匯入完成！")
    except Exception as e:
        print(f"❌ MySQL 匯入失敗：{e}")
        traceback.print_exc()
