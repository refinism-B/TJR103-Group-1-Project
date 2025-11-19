import os

import pandas as pd
import pymysql
from colorama import Fore
from dotenv import load_dotenv
from utils import connectDB as conn_db
from utils import extractdata as ed
from utils import readdata as rd


def clean(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    return v


def main():
    # 載入.env檔案
    load_dotenv()

    # csv檔路徑
    df = rd.get_csv_data("/opt/airflow/data/complete/store/type=hotel/store.csv")

    print("=== HOTEL RAW PHONE BEFORE ANY PROCESSING ===")
    print(df["phone"].head(20))
    print(df["phone"].isna().sum(), "/", len(df))

    df["phone"] = df["phone"].astype(str)

    # csv讀取後手機格式會跑掉，透過函式做轉換
    df["phone"] = df["phone"].apply(ed.to_phone)

    df = df.astype(object).where(pd.notnull(df), None)

    # 避免空值
    for col in df.columns:
        df[col] = df[col].apply(ed.to_sql_null)

    # 設定資料庫連線
    host = os.getenv("MYSQL_IP")
    port = int(os.getenv("MYSQL_PORTT"))
    user = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    db = os.getenv("MYSQL_DB_NAME")

    # 建立連線
    conn, cursor = conn_db.connect_db(host, port, user, password, db)

    try:
        # 寫入資料
        count = 0  # 計算幾筆資料

        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("TRUNCATE TABLE hotel;")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        for _, row in df.iterrows():
            sql = """
            INSERT INTO hotel(
                id, name, buss_status, loc_id, address, phone, op_hours, category_id, rating, rating_total, newest_review, longitude, latitude, map_url, website, place_id
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            );
            """
            clean_row = tuple(clean(v) for v in row)
            count += cursor.execute(sql, clean_row)  # pymysql以tuple傳送資料

        # 提交資料
        conn.commit()
        print(Fore.GREEN + f"✅ 資料已新增完畢，一共新增{count}筆資料")
    except pymysql.err.ProgrammingError as e:
        print(Fore.RED + "❌ SQL 語法錯誤：", e)
    except pymysql.err.DataError as e:
        print(Fore.RED + "❌ 資料型態錯誤：", e)
    except pymysql.err.IntegrityError as e:
        print(Fore.RED + "❌ 主鍵/外鍵/唯一性衝突：", e)
    except Exception as e:
        print(Fore.RED + "❌ 其他錯誤：", e)
    finally:
        if conn and conn.open:
            cursor.close()
            conn.close()
            print(Fore.YELLOW + "🔒 連線已關閉")


if __name__ == "__main__":
    main()
