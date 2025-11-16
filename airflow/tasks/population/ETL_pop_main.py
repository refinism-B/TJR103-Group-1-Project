import os
import sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# === 加入專案根目錄 ===
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 正確 import（你的 E_pop.py 的真正函式名稱） ===
from tasks.population.E_pop import fetch_population_data
from tasks.population.T_pop import transform
from tasks.population.L_pop import load

# === 載入 .env ===
load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env"))


def get_engine():
    username = os.getenv("MYSQL_USERNAME")
    password = os.getenv("MYSQL_PASSWORD")
    target_ip = os.getenv("MYSQL_IP")
    target_port = os.getenv("MYSQL_PORT")
    db_name = os.getenv("MYSQL_DB_NAME")

    if not all([username, password, target_ip, target_port, db_name]):
        raise ValueError("❌ .env 資訊不完整，請確認 MySQL 連線參數")

    return create_engine(f"mysql+pymysql://{username}:{password}@{target_ip}:{target_port}/{db_name}")


def save_to_local(df, raw_path, processed_path):
    os.makedirs(os.path.dirname(raw_path), exist_ok=True)
    os.makedirs(os.path.dirname(processed_path), exist_ok=True)

    df.to_csv(raw_path, index=False, encoding="utf-8-sig")
    print(f"📦 [L1] 已成功輸出原始資料：{raw_path}")

    df.to_csv(processed_path, index=False, encoding="utf-8-sig")
    print(f"📦 [L1] 已成功輸出處理後資料：{processed_path}")


def save_to_db(df, table_name):
    try:
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(f"CREATE TABLE IF NOT EXISTS {table_name} LIKE population;"))
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
            df.to_sql(table_name, con=conn, if_exists="append", index=False)

        print(f"💾 [L2] 已成功匯入 MySQL 資料表：{table_name}")

    except Exception as e:
        print(f"❌ 匯入 MySQL 失敗：{e}")


def load(df):
    base_dir = os.path.join(PROJECT_ROOT, "airflow", "data")
    raw_path = os.path.join(base_dir, "raw", "population", "population_raw.csv")
    processed_path = os.path.join(base_dir, "processed", "population", "population_processed.csv")
    table_name = "population_new"

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(save_to_local, df, raw_path, processed_path)
        executor.submit(save_to_db, df, table_name)


def main():
    print("📊 [E] Extract - 抓取內政部人口統計資料中...")

    # ⭐ 設定下載資料夾：airflow/data/raw/population
    raw_dir = os.path.join(PROJECT_ROOT, "airflow", "data", "raw", "population")
    os.makedirs(raw_dir, exist_ok=True)

    # ⭐ 正確呼叫 Extract 函式
    xls_path, year, month = fetch_population_data(raw_dir)

    print(f"📄 最新下載檔案：{xls_path}")
    print(f"📅 資料年月：{year}/{month}")

    print("⚙️ [T] Transform - 清理與整合資料中...")

    # ⭐ read excel 再進 Transform
    df_raw = pd.read_excel(xls_path, header=1)
    # 刪除全部 Unnamed 欄位
    df_raw = df_raw.loc[:, ~df_raw.columns.str.contains("Unnamed")]

    df_processed = transform(df_raw)

    print("💾 [L] Load - 地端存檔與 DB 匯入（平行進行）中...")
    load(df_processed)

    print("🎉 ETL Population Pipeline 完成！")


if __name__ == "__main__":
    main()
