import os
import pandas as pd
from E_shelter import fetch_raw_data
from T_shelter import transform
from L_shelter import load

def main():
    print("🐾 [E] Extract - 抓取農業部資料中...")
    df_raw = fetch_raw_data()

    print(f"📋 df_raw 欄位：{df_raw.columns.tolist()}")
    print(f"✅ 原始資料筆數：{len(df_raw)}")

    print("⚙️ [T] Transform - 清理與 Google 資料整合中...")
    df_processed = transform(df_raw)

    if df_processed is None or df_processed.empty:
        print("⚠️ df_processed 為空或 None，跳過匯入 MySQL")
        return

    print(f"✅ 處理後資料筆數：{len(df_processed)}")

    print("💾 [L] Load - 匯入 MySQL 中...")
    load(df_processed)

    print("🎉 ETL Shelter Pipeline 完成！")

if __name__ == "__main__":
    main()