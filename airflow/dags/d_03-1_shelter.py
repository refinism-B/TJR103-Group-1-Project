import os
import sys
sys.path.append('/opt/airflow/tasks')
sys.path.append('/opt/airflow/utils')
sys.path.append('/opt/airflow/drivers')
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ==========================================================
# 設定專案根目錄，讓 Airflow 能 import tasks.shelter.*
# ==========================================================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))  # airflow/ 的上一層

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ==========================================================
# 匯入 ETL 主流程
# ==========================================================
from tasks.shelter.ETL_shelter_main import main as etl_shelter_main

# ==========================================================
# DAG 參數
# ==========================================================
default_args = {
    "owner": "airflow",
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
    dag_id="d03_1_etl_shelter",
    description="Shelter ETL Pipeline",
    schedule="@monthly",
    start_date=datetime(2024, 12, 1),
    catchup=False,
    default_args=default_args,
    tags=["517", "shelter", "monthly", "google_API"],
) as dag:

    etl_shelter_task = PythonOperator(
        task_id="run_shelter_etl",
        python_callable=etl_shelter_main,
    )

    etl_shelter_task
