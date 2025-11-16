from datetime import date, datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from tasks import database_file_mod as dfm

# 設定DAG基本資訊
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="d_01-11_compute_p",
    default_args=default_args,
    description="[每日更新]計算p分數",
    schedule_interval="0 20 5 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["sonia", "bevis", "monthly", "analyze"]
)
def d_01_11_compute_p():
    @task
    def T_clean_data(df: list[dict]) -> pd.DataFrame:
        df = pd.DataFrame(data=df)

        df = df.dropna(subset=["city", "district", "category_id"])
        df["category_id"] = df["category_id"].astype(int)
        df["category_raw_score"] = df["category_raw_score"].fillna(0.0)

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

    @task
    def T_city_normalize(df: pd.DataFrame) -> pd.DataFrame:
        grp_city = df.groupby(["city", "category_id"])["category_raw_score"]
        p10_city = grp_city.transform(lambda s: s.quantile(0.10))
        p90_city = grp_city.transform(lambda s: s.quantile(0.90))
        df["norm_city"] = normalize_series(
            df["category_raw_score"], p10_city, p90_city)
        df["norm_city"] = df["norm_city"].round(2)

        return df

    @task
    def T_metro_normalize(df: pd.DataFrame) -> pd.DataFrame:
        grp_metro = df.groupby(["category_id"])["category_raw_score"]
        p10_metro = grp_metro.transform(lambda s: s.quantile(0.10))
        p90_metro = grp_metro.transform(lambda s: s.quantile(0.90))
        df["norm_metro"] = normalize_series(
            df["category_raw_score"], p10_metro, p90_metro)
        df["norm_metro"] = df["norm_metro"].round(2)

        return df

    @task
    def T_select_columns(df: pd.DataFrame, col_list: list) -> pd.DataFrame:
        df_save = df[col_list]

        return df_save
#
#
#
#
#
    sql = """
    select
        loc_id,
        city,
        district,
        category_id,
        category_raw_score
    from v_5category_raw_district
    """

    df = dfm.E_query_from_sql(sql=sql)

    df = T_clean_data(df=df)

    df = T_city_normalize(df=df)

    df = T_metro_normalize(df=df)

    col_list = ["loc_id", "city", "district", "category_id",
                "category_raw_score", "norm_city", "norm_metro"]
    df_save = T_select_columns(df=df, col_list=col_list)


d_01_11_compute_p()
