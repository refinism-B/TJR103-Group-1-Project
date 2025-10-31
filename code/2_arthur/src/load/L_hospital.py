import pymysql
import os
from colorama import Fore
from mods import readdata as rd
from mods import connectDB as conn_db
from mods import extractdata as ed
from dotenv import load_dotenv

# 載入.env檔案
load_dotenv()

# csv檔路徑
df = rd.get_csv_data("data/processed/hospital_data_final.csv")

# csv讀取後手機格式會跑掉，透過函式做轉換
df = ed.to_phone(df)

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
    for _, row in df.iterrows():
        sql = """
        INSERT INTO Hospital (
            hospital_id, place_id, name, address, phone, city, district, loc_id,
            business_status, opening_hours, types, rating, rating_total,
            longitude, latitude, map_url, website, newest_review
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        );
        """
        count += cursor.execute(sql, tuple(row))  # pymysql以tuple傳送資料

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
