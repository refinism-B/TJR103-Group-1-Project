import os
import sys

# === 自動加入專案根目錄 TJR103group1 ===
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# === 正確使用 package import ===
from tasks.shelter.E_shelter import fetch_raw_data
from tasks.shelter.T_shelter import transform
from tasks.shelter.L_shelter import load


def main():
    print("🐾 [E] Extract - 抓取農業部資料中...")
    df_raw = fetch_raw_data()

    print("⚙️ [T] Transform - 清理與 Google 資料整合中...")
    df_processed = transform(df_raw)

    print("💾 [L] Load - 匯入 MySQL 中...")
    load(df_processed)

    print("🎉 ETL Shelter Pipeline 完成！")


if __name__ == "__main__":
    main()
