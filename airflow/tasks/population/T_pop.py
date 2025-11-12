import pandas as pd

def transform(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    處理人口資料欄位名稱、移除空白列、統一縣市格式等。
    回傳清理後的 DataFrame。
    """
    print("🧹 開始清理人口資料...")

    # 1️⃣ 去除空白欄位與重複列
    df = df_raw.dropna(how='all').drop_duplicates()

    # 2️⃣ 標準化欄位名稱
    df.columns = (
        df.columns.str.strip()
        .str.replace("\n", "_", regex=False)
        .str.replace(" ", "_", regex=False)
    )

    # 3️⃣ 過濾有效欄位 (假設有縣市、鄉鎮、市區人口)
    keep_cols = [col for col in df.columns if "區" in col or "鄉" in col or "人口" in col or "縣" in col]
    if keep_cols:
        df = df[keep_cols]

    # 4️⃣ 加入處理時間戳記
    df["etl_timestamp"] = pd.Timestamp.now()

    print("✅ Transform 完成！")
    return df
