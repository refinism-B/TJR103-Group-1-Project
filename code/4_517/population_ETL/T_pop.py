"""
T_pop.py
解析 XLS → 清理 → 產出六都人口 CSV
"""

import os
import re
import pandas as pd


def transform_population(xls_path: str, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    print("📖 開始解析各縣市資料...")

    city_rows = {
        "臺北市": 12,
        "新北市": 31,
        "桃園市": 13,
        "臺中市": 29,
        "臺南市": 37,
        "高雄市": 38
    }

    xls = pd.ExcelFile(xls_path)
    df_all = pd.DataFrame()

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
            names=["行政區", "戶數", "人口數"]
        )

        df = df.dropna(subset=["行政區"])
        df = df[~df["行政區"].astype(str).str.contains("合計|總計|註|^說明")]

        df["行政區"] = (
            df["行政區"]
            .astype(str)
            .str.replace("※", "")
            .apply(lambda x: re.sub(r"\s+", "", x))
            .str.strip()
        )
        df["人口數"] = pd.to_numeric(df["人口數"].astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)
        df.insert(0, "縣市", city)
        df = df[["縣市", "行政區", "人口數"]]
        df_all = pd.concat([df_all, df], ignore_index=True)
        print(f"✅ {city} 已擷取 {len(df)} 筆資料")

    csv_name = "six_city_population.csv"
    output_path = os.path.join(output_dir, csv_name)
    df_all.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"📦 已成功輸出：{output_path}")
    return output_path
