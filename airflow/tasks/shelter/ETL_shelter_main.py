# ETL_shelter_main.py
from E_shelter import fetch_raw_data
from T_shelter import transform
from L_shelter import load

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
