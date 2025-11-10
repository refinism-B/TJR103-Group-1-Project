import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


def save_population_csv(df, processed_dir):
    """
    將清理後的人口資料儲存至 data/processed/population/pop_etl.csv
    （不包含 month 欄位）
    """
    output_path = os.path.join(processed_dir, "pop_processed.csv")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"📦 已成功輸出人口數據：{output_path}")
    return output_path


def save_to_mysql(df):
    """
    將人口資料匯入 MySQL 資料庫（不包含 month 欄位）
    """
    load_dotenv()
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = int(os.getenv("MYSQL_PORTT"))
    db_name = os.getenv("MYSQL_DB_NAME")

    try:
        engine = create_engine(f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}")
        df.to_sql(name="population", con=engine, if_exists="replace", index=False)
        print("✅ 資料已成功匯入 MySQL 資料表：population")
    except Exception as e:
        print(f"❌ MySQL 匯入失敗：{e}")
