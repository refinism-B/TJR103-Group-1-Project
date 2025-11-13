import pandas as pd
from airflow.decorators import dag, task
from datetime import datetime, date, timedelta
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
    dag_id="d_01-10_compute_m",
    default_args=default_args,
    description="[每日更新]計算m分數",
    schedule_interval="0 20 5 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "monthly", "location"]
)
def d_01_10_compute_m():
    @task
    def


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
        from v_fact_store_all as f
        left join location as l 
        on f.loc_id = l.loc_id;
        """



"/opt/airflow/data/data/complete/store/type=hospital/store.csv"