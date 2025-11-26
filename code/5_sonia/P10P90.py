
# 從v_5category_raw_district讀 category_raw_score
# 依市內(city)跟類別(category_id)做 P10–P90 標準化= norm_city
# 依六都(全部城市)跟類別(category_id)做 P10–P90 標準化 =norm_metro
# 寫回 mysql：A_7category_score_norm（含 loc_id, city, district, category_id, category_raw_score, norm_city, norm_metro）

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from datetime import date

load_dotenv()

# mysql連線設定
user = os.getenv("MYSQL_USERNAME")
password = os.getenv("MYSQL_PASSWORD")
host = os.getenv("MYSQL_IP")
port = int(os.getenv("MYSQL_PORTT"))
database = os.getenv("MYSQL_DB_NAME")


engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
)

# 從v_5category_raw_district匯入行政區類別原始分數
sql_raw = """
select
    loc_id,
    city,
    district,
    category_id,
    category_raw_score
from v_5category_raw_district
"""
df = pd.read_sql(sql_raw, engine)

# 避免後面出現空值或型別怪怪的
df = df.dropna(subset=["city", "district", "category_id"])      # key 不可為空
df["category_id"] = df["category_id"].astype(int)               # 類別轉int
df["category_raw_score"] = df["category_raw_score"].fillna(0.0) # raw分數補0

# 定義：把一組數據用P10–P90 回傳0.5-9.5
def normalize_series(x: pd.Series, p10: pd.Series, p90: pd.Series) -> pd.Series:
    """
    x   : 要轉換的原始分數（Series）
    p10 : 同長度的第10百分位數（Series，已對齊 x）
    p90 : 同長度的第90百分位數（Series，已對齊 x）
    回傳：回傳到 0.5-9.5 的分數
    """
    # 避免 P90==P10 造成除0，先把相等的分母換成NaN
    denom = (p90 - p10).replace(0, pd.NA)
    ratio = (x - p10) / denom
    # 若分母為NaN（等於 0 的情況），或原本就NaN視為0
    ratio = ratio.fillna(0.0)
    # 夾在 [0, 1]
    ratio = ratio.clip(0, 1)
    # 回傳到 [0.5, 9.5]
    return 0.5 + ratio * 9.0

# 市內標準化（同城市同類別
# groupby 之後對每個群組計算 P10以及P90，再transform 回到每列
grp_city = df.groupby(["city", "category_id"])["category_raw_score"]
p10_city  = grp_city.transform(lambda s: s.quantile(0.10))
p90_city  = grp_city.transform(lambda s: s.quantile(0.90))
df["norm_city"] = normalize_series(df["category_raw_score"], p10_city, p90_city)

# 六都標準化（全城市合併 依類別分組
grp_metro = df.groupby(["category_id"])["category_raw_score"]
p10_metro = grp_metro.transform(lambda s: s.quantile(0.10))
p90_metro = grp_metro.transform(lambda s: s.quantile(0.90))
df["norm_metro"] = normalize_series(df["category_raw_score"], p10_metro, p90_metro)

# 保留2位小數
df["norm_city"]  = df["norm_city"].round(2)
df["norm_metro"] = df["norm_metro"].round(2)

# copy準備寫回mysql
out_cols = ["loc_id", "city", "district", "category_id", "category_raw_score", "norm_city", "norm_metro"]
out_df = df[out_cols].copy()

today = date.today()
out_df["update_date"] = today

# 存回category_score_norm
with engine.begin() as conn:
    # 若表已存在會直接覆蓋（不保留舊資料）
    conn.exec_driver_sql("drop table if exists A_7category_score_norm")
    out_df.to_sql("A_7category_score_norm", con=conn, if_exists="replace", index=False)

print(f"rows written: {len(out_df)}")
print(out_df.head())
