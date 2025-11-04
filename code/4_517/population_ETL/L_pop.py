"""
L_pop.py
將人口資料寫入 MySQL 資料庫
"""

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd


def load_population(csv_path: str):
    load_dotenv()
    df = pd.read_csv(csv_path)

    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = int(os.getenv("MYSQL_PORTT"))
    db_name = os.getenv("MYSQL_DB_NAME")

    print(f"🗄️ 寫入 MySQL 資料庫：{db_name}.raw_population")
    engine = create_engine(f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}")

    df.to_sql(name="raw_population", con=engine, if_exists="replace", index=False)
    print(f"✅ 已成功匯入 {len(df)} 筆資料至 {db_name}.raw_population")
