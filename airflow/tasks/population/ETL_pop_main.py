# =============================================
# 👥 Taiwan Population ETL Main (Final Version)
# =============================================

import os, sys
import pandas as pd
from dotenv import load_dotenv

# === 自動修正路徑 ===
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# === Import ETL Modules ===
from population_ETL.E_pop import fetch_population_data
from population_ETL.T_pop import transform_population_data
from population_ETL.L_pop import save_population_csv, save_to_mysql

# === Load env ===
load_dotenv()

# === Directory setup ===
base_dir = os.path.join(os.getcwd(), "data")
raw_dir = os.path.join(base_dir, "raw", "population")
processed_dir = os.path.join(base_dir, "processed", "population")
os.makedirs(raw_dir, exist_ok=True)
os.makedirs(processed_dir, exist_ok=True)


def main():
    print("👥 Population ETL process starting...\n")

    # 1️⃣ Extract - Download XLS & save to raw
    xls_path, latest_year, latest_month = fetch_population_data(raw_dir)
    if not xls_path:
        print("❌ 無法取得人口資料，流程結束。")
        return

    # 2️⃣ Transform - Clean & merge
    df = transform_population_data(xls_path, latest_year, latest_month)

    # 3️⃣ Load - Save to processed folder
    output_path = save_population_csv(df, processed_dir)

    # 4️⃣ Load - Save to MySQL
    save_to_mysql(df)

    print(f"\n✅ Population ETL process completed successfully.")
    print(f"📦 Output file: {output_path}")


if __name__ == "__main__":
    main()
