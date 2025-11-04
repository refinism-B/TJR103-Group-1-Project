import pandas as pd
from sqlalchemy import create_engine
from config import OUTPUT_FILE, MYSQL

def save_shelter_to_csv(df):
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"📊 已輸出 CSV：{OUTPUT_FILE}")

def save_shelter_to_mysql(df):
    try:
        engine = create_engine(
            f"mysql+pymysql://{MYSQL['username']}:{MYSQL['password']}@{MYSQL['ip']}:{MYSQL['port']}/{MYSQL['db_name']}"
        )
        df.to_sql(name="pet_shelter", con=engine, if_exists="replace", index=False)
        print("✅ 已匯入 MySQL 資料表：pet_shelter")
    except Exception as e:
        print(f"❌ 匯入 MySQL 失敗：{e}")
