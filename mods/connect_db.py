import pymysql
from colorama import Fore


def connect_db(host: str, port: int, user: str, password: str, db: str, charset: str):
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
            host=host, port=port, user=user, passwd=password, db=db, charset=charset
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
