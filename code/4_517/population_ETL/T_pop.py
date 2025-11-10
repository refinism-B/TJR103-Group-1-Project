import os
import re
import pandas as pd


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
        df["population"] = pd.to_numeric(df["population"].astype(str).str.replace(",", ""), errors="coerce").fillna(0).astype(int)
        df.insert(0, "city", city)
        df = df[["city", "district", "population"]]

        df_all = pd.concat([df_all, df], ignore_index=True)
        print(f"✅ {city} 已擷取 {len(df)} 筆資料")

    # ✅ 仍保留 month 作為內部紀錄，不輸出
    df_all["month"] = f"{latest_year}{str(latest_month).zfill(2)}"

    total_rows = len(df_all)
    if total_rows != 158:
        print(f"⚠️ 資料筆數不符：目前為 {total_rows} 筆，預期為 158 筆")
    else:
        print("✅ 資料筆數正確，共 158 筆")

    # ✅ 輸出前移除 month 欄位
    df_export = df_all[["city", "district", "population"]].copy()

    return df_export
