# d_03-2_population.py


import os
import sys

sys.path.append("/opt/airflow/tasks")
sys.path.append("/opt/airflow/utils")
sys.path.append("/opt/airflow/drivers")
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

from airflow import DAG

# ==========================================================
# 設定專案根目錄 (airflow 的上一層)
# ==========================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))  # airflow/ 的上一層

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ==========================================================
# 匯入人口 ETL 模組
# ==========================================================
from tasks.population.E_pop import fetch_raw_data
from tasks.population.L_pop import load
from tasks.population.T_pop import transform_population_data

# ==========================================================
# 預設參數
# ==========================================================
default_args = {
    "owner": "Ken",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# ==========================================================
# DAG 設定
# ==========================================================
with DAG(
    dag_id="d03_2_population",
    description="Population ETL Pipeline (with MySQL location mapping)",
    default_args=default_args,
    schedule="@monthly",  # 或 None, 或 cron 表達式
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags=["population", "ETL", "monthly"],
) as dag:

    # --------------------------
    # Extract
    # --------------------------
    def extract_task():
        print("📊 [E] Extract - 抓取內政部人口統計資料中...")
        fetch_raw_data("/opt/airflow/data/raw/population")
        print("✅ 已抓取原始人口資料")

    # --------------------------
    # Transform
    # --------------------------
    def transform_task():

        print("⚙️ [T] Transform - 清理並對應 MySQL location...")
        # TODO 要手動修改檔名
        df_processed = transform_population_data(
            "/opt/airflow/data/raw/population/鄉鎮戶數及人口數-114年10月.xls"
        )
        print(f"✅ 已轉換人口資料，共 {len(df_processed)} 筆")

    # --------------------------
    # Load
    # --------------------------
    def load_task():
        import pandas as pd

        df = pd.read_csv(
            "/opt/airflow/data/data/complete/store/type=population/store.csv"
        )

        print("💾 [L] Load - 匯入 MySQL 中...")
        load(df)
        print("🎉 Population ETL Pipeline 完成！")

    # ==========================================================
    # Airflow Tasks
    # ==========================================================
    extract_population = PythonOperator(
        task_id="extract_population",
        python_callable=extract_task,
    )

    transform_population = PythonOperator(
        task_id="transform_population",
        python_callable=transform_task,
    )

    load_population = PythonOperator(
        task_id="load_population",
        python_callable=load_task,
    )

    # 任務順序
    extract_population >> transform_population >> load_population
