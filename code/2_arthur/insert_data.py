import pymysql
from colorama import Fore
from mods import get_var_data as gd


# csv檔路徑
file_path = "data/processed/hospital_all_ETL.csv"

# 讀取要insert的df
df = gd.get_csv_data(file_path)
columns_to_insert = ["name", "address", "opening_hour"]
df_filtered = df[columns_to_insert]

# 設定資料庫連線
host = "localhost"
port = 8888
user = "root"
password = "pet8888"
db = "PET"
charset = "utf8mb4"

# 建立連線
try:
    conn = pymysql.connect(
        host=host, port=port, user=user, passwd=password, db=db, charset=charset
    )
    print(Fore.GREEN + f"✅ {db}資料庫已成功連線")

    # 建立cursor
    cursor = conn.cursor()

    # 寫入資料
    count = 0  # 計算幾筆資料
    for _, row in df_filtered.iterrows():
        sql = """
        insert into hospital (name, address, opening_hour)
        values (%s, %s, %s)
        """
        count += cursor.execute(sql, tuple(row))  # pymysql以tuple傳送資料

    # 提交資料
    conn.commit()
    print(Fore.GREEN + f"✅ 資料已新增完畢，一共新增{count}筆資料")

except pymysql.MySQLError as e:
    print(Fore.RED + "❌ 連線錯誤：", e)
except pymysql.err.OperationalError as e:
    print(Fore.RED + "❌ 連線或權限問題：", e)
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
