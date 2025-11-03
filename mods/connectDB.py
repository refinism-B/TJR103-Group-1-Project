"""
與mysql連線的套件
Creator: Chgwyellow

from mods import connectDB as conn_db
"""

import pymysql
import pandas as pd
from colorama import Fore


def connect_db(host: str, port: int, user: str, password: str, db: str):
    """對資料庫做連線
    請使用.env讀取的內容做為參數傳入

    Args:
        host (str): 主機名稱
        port (str): 埠號
        user (str): 使用者名稱
        password (str): 使用者密碼
        db (str): 資料庫名稱
        charset (str): 字元集

    Returns:
        _type_: 自動建立資料庫連線且成功時回傳conn與cursor
    """

    conn = None
    cursor = None

    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=db,
            charset="utf8mb4",
        )
        print(Fore.GREEN + f"✅ {db}資料庫已成功連線")

        # 建立cursor
        cursor = conn.cursor()
        return conn, cursor
    except pymysql.MySQLError as e:
        print(Fore.RED + "❌ 連線錯誤：", e)
    except pymysql.err.OperationalError as e:
        print(Fore.RED + "❌ 連線或權限問題：", e)
    except Exception as e:
        print(Fore.RED + "❌ 其他錯誤：", e)
    return None, None


def get_loc_table(conn, cursor) -> pd.DataFrame:
    """從DB取回location table的loc_id, city, district

    Args:
        conn: MySQL連線資訊
        cursor: pymysql的cursor物件

    Returns:
        pd.DataFrame: 包含loc_id, city和district的DataFrame
    """

    sql = """
    select loc_id, city, district
    from location;
    """
    try:
        cursor.execute(sql)
        loc = cursor.fetchall()
        df = pd.DataFrame(data=loc, columns=["loc_id", "city", "district"])
        print(Fore.GREEN + "✅ location資料已取回")
        return df
    except pymysql.err.OperationalError as e:
        conn.rollback()
        # 可實作重連/重試
        print(Fore.RED + "❌ OperationalError:", e)
    except pymysql.err.IntegrityError as e:
        conn.rollback()
        # 資料違反約束，記錄錯誤供人工處理
        print(Fore.RED + "❌ IntegrityError:", e)
    except pymysql.MySQLError as e:
        conn.rollback()
        print(Fore.RED + "❌ MySQLError:", e)
    except Exception as e:
        # 其他非預期錯誤
        print(Fore.RED + "❌ Unexpected error:", e)