from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.tasks import etl_pop

# -------------------------------------
# ✨ Step 1. DAG 參數設定
# -------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email": ["your_email@example.com"],  # ← 可改為團隊信箱
    "email_on_failure": False,
    "email_on_retry": False,
}

# -------------------------------------
# ✨ Step 2. 建立 DAG
# -------------------------------------
with DAG(
    dag_id="etl_population_dag",
    description="Population ETL pipeline (single-task wrapper)",
    schedule_interval="@monthly",  # 每月執行
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["population", "etl"],
) as dag:

    # -------------------------------------
    # ✨ Step 3. 定義 Airflow 任務
    # -------------------------------------
    etl_task = PythonOperator(
        task_id="etl_population_pipeline",
        python_callable=etl_pop.main,  # 呼叫完整 ETL 任務
    )

    etl_task
