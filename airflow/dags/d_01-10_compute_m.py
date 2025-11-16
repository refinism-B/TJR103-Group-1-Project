import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, date, timedelta
from tasks import database_file_mod as dfm
from tasks import pandas_mod as pdm


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
    dag_id="d_01-10_compute_m",
    default_args=default_args,
    description="[每日更新]計算m分數",
    schedule_interval="0 20 5 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["sonia", "bevis", "monthly", "analyze"]
)
def d_01_10_compute_m():
    @task
    def T_calculate_P75(df_store: pd.DataFrame, df_stats: pd.DataFrame) -> pd.DataFrame:
        p75_district_cat = (
            df_store
            .groupby(["city", "district", "category_id"], as_index=False)["reviews"]
            .quantile(0.75)
            .rename(columns={"reviews": "p75_district_cat"})
        )

        # city, category
        p75_city_cat = (
            df_store
            .groupby(["city", "category_id"], as_index=False)["reviews"]
            .quantile(0.75)
            .rename(columns={"reviews": "p75_city_cat"})
        )

        # 合併city,district P75
        merged = (
            df_stats
            .merge(p75_district_cat, on=["city", "district", "category_id"], how="left")
            .merge(p75_city_cat, on=["city", "category_id"], how="left")
        )

        return merged

    @task
    def T_merged_fillna(df: pd.DataFrame) -> pd.DataFrame:
        df["p75_district_cat"] = df["p75_district_cat"].fillna(0)
        df["p75_city_cat"] = df["p75_city_cat"].fillna(0)

        return df

    @task
    def T_calculate_weight_and_mscore(df: pd.DataFrame, t: int) -> pd.DataFrame:
        # 權重w：樣本越多（n_district_cat 大），越信任district的 P75
        df["w_district_cat"] = df["n_district_cat"] / \
            (df["n_district_cat"] + t)

        # m = w * P75_district_cat + (1 - w) * P75_city_cat
        df["m_city_district_cat"] = (
            df["w_district_cat"] * df["p75_district_cat"] +
            (1 - df["w_district_cat"]) * df["p75_city_cat"]
        )

        result = df[["city", "district", "category_id",
                     "m_city_district_cat"]].copy()

        return result
#
#
#
#
#
#
    sql_stores = """
        select
            f.category_id,
            f.rating_total as reviews,
            l.city,
            l.district
        from v_2fact_store_all as f
        left join location as l
        on f.loc_id = l.loc_id;
        """

    df_store_dict = dfm.E_query_from_sql(sql=sql_stores)
    df_store = pdm.T_transform_to_df(data=df_store_dict)

    sql_stats = """
        SELECT
            city, district, category_id, n_district_cat
        FROM v_3district_cat_stats;
        """

    df_stats_dict = dfm.E_query_from_sql(sql=sql_stats)
    df_stats = pdm.T_transform_to_df(data=df_stats_dict)

    df_merged = T_calculate_P75(df_store=df_store, df_stats=df_stats)

    df_merged = T_merged_fillna(df=df_merged)

    df_result = T_calculate_weight_and_mscore(df=df_merged, t=30)

    dfm.L_truncate_and_upload_data_to_db(
        df=df_result, table_name="A_agg_district_cat_m")


d_01_10_compute_m()
