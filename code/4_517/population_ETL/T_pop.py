# T_pop.py

import os
import re
import pandas as pd

# === 🆕 最終輸出路徑（store.csv） ===
FINAL_OUTPUT_DIR = "/opt/airflow/data/data/complete/store/type=population"
os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)
FINAL_OUTPUT_PATH = os.path.join(FINAL_OUTPUT_DIR, "store.csv")


def transform_population_data(xls_path, latest_year, latest_month):
    """
    從 XLS 解析六都人口資料，清理後回傳 DataFrame
    """
    print("📖 開始解析各縣市資料...")
    xls = pd.ExcelFile(xls_path)
    df_all = pd.DataFrame()

    city_rows = {
        "臺北市": 12, "新北市": 31, "桃園市": 13,
        "臺中市": 29, "臺南市": 37, "高雄市": 38
    }

    for city, row_count in city_rows.items():
        if city not in xls.sheet_names:
            print(f"⚠️ 找不到工作表：{city}")
            continue

        df = pd.read_excel(
            xls,
            sheet_name=city,
            skiprows=4,
            nrows=row_count,
            usecols="A:C",
            header=None,
            names=["district", "household", "population"]
        )

        df = df.dropna(subset=["district"])
        df = df[~df["district"].astype(str).str.contains("合計|總計|註|^說明")]
        df["district"] = (
            df["district"]
            .astype(str)
            .str.replace("※", "")
            .apply(lambda x: re.sub(r"\s+", "", x))
            .str.strip()
        )
        df["population"] = pd.to_numeric(
            df["population"].astype(str).str.replace(",", ""),
            errors="coerce"
        ).fillna(0).astype(int)

        df.insert(0, "city", city)
        df = df[["city", "district", "population"]]

        df_all = pd.concat([df_all, df], ignore_index=True)
        print(f"✅ {city} 已擷取 {len(df)} 筆資料")

    # 內部紀錄
    df_all["month"] = f"{latest_year}{str(latest_month).zfill(2)}"

    total_rows = len(df_all)
    if total_rows != 158:
        print(f"⚠️ 資料筆數不符：目前為 {total_rows} 筆，預期為 158 筆")
    else:
        print("✅ 資料筆數正確，共 158 筆")

    # 輸出欄位
    df_export = df_all[["city", "district", "population"]].copy()

    # === 🆕 最終輸出 store.csv ===
    df_export.to_csv(FINAL_OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print(f"📦 最終完整輸出：{FINAL_OUTPUT_PATH}")

    return df_export
