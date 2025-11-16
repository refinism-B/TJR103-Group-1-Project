import pandas as pd
from sqlalchemy import create_engine

# mysql連線設定
user = "sonia"
password = "pet88888"
host = "35.194.236.122"
port = 3306
database = "TJR103_1"

engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
)

# 平滑常數 t
# t 越大=越相信city P75,越小=越相信district P75
t = 30

#讀取店家評論數以及行政區資料（v_fact_store_all + location）
sql_stores = """
select
    f.category_id,
    f.rating_total as reviews,
    l.city,
    l.district
from v_fact_store_all as f
left join location as l 
  on f.loc_id = l.loc_id;
"""
stores = pd.read_sql(sql_stores, engine)


# 讀行政區跟類別的樣本數（v_district_cat_stats）
sql_stats = """
SELECT
    city, district, category_id, n_district_cat
FROM v_district_cat_stats;
"""
stats = pd.read_sql(sql_stats, engine)

#計算 city 跟 district 的P75
#階層（city, district, category）
p75_district_cat = (
    stores
      .groupby(["city", "district", "category_id"], as_index=False)["reviews"]
      .quantile(0.75)
      .rename(columns={"reviews": "p75_district_cat"})
)

# city, category
p75_city_cat = (
    stores
      .groupby(["city", "category_id"], as_index=False)["reviews"]
      .quantile(0.75)
      .rename(columns={"reviews": "p75_city_cat"})
)

# 合併city,district P75
merged = (
    stats
      .merge(p75_district_cat, on=["city", "district", "category_id"], how="left")
      .merge(p75_city_cat, on=["city", "category_id"], how="left")
)

# 缺值補0
merged["p75_district_cat"] = merged["p75_district_cat"].fillna(0)
merged["p75_city_cat"] = merged["p75_city_cat"].fillna(0)

# 權重w與m值
# 權重w：樣本越多（n_district_cat 大），越信任district的 P75
merged["w_district_cat"] = merged["n_district_cat"] / (merged["n_district_cat"] + t)

# m = w * P75_district_cat + (1 - w) * P75_city_cat
merged["m_city_district_cat"] = (
    merged["w_district_cat"] * merged["p75_district_cat"] +
    (1 - merged["w_district_cat"]) * merged["p75_city_cat"]
)

result = merged[["city", "district", "category_id", "m_city_district_cat"]].copy()


# 覆蓋到 agg_district_cat_m
with engine.begin() as conn:
    conn.exec_driver_sql("truncate table A_agg_district_cat_m")
    result.to_sql("A_agg_district_cat_m", con=conn, if_exists="append", index=False)