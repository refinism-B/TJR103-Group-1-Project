import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pymysql

# === 初始化環境變數 ===
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

# === 讀取設定 ===
def get_engine():
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = os.getenv("MYSQL_PORT")
    db_name = os.getenv("MYSQL_DB_NAME")

    if not all([username, password, target_ip, target_port, db_name]):
        raise ValueError("❌ .env 資訊不完整，請確認 MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_IP, MYSQL_PORT, MYSQL_DB_NAME")

    try:
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}")
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"✅ 已成功連線至 MySQL：{target_ip}:{target_port}/{db_name}")
        return engine
    except Exception as e:
        raise ConnectionError(f"❌ 無法連線至 MySQL：{e}")

# === 匯入資料 ===
def load(df: pd.DataFrame):
    print("💾 [L] Load - 匯入 MySQL 中...")

    try:
        engine = get_engine()
    except Exception as e:
        print(f"❌ 無法建立連線：{e}")
        return

    # 確保欄位存在 website
    if "website" not in df.columns:
        df["website"] = ""

    # 確保欄位順序一致
    expected_columns = [
        "id", "name", "buss_status", "loc_id", "address", "phone",
        "op_hours", "category_id", "rating", "rating_total",
        "newest_review", "longitude", "latitude", "map_url",
        "website", "place_id", "update_time"
    ]
    df = df[[c for c in expected_columns if c in df.columns]]

    try:
        df.to_sql(name="shelter", con=engine, if_exists="replace", index=False)
        print("✅ 已成功匯入 MySQL 資料表：shelter")
    except Exception as e:
        print(f"❌ 匯入失敗：{e}")
