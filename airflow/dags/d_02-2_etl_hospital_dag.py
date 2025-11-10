from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from tasks.hospital import (
    E_hospital,
    L_hospital,
    T_hospital_c_d,
    T_hospital_cat_id,
    T_hospital_clean_sort,
    T_hospital_details,
    T_hospital_id,
    T_hospital_merge,
    T_hospital_place_id,
    T_hospital_sql,
)

from airflow import DAG

# -------------------------------------
# ✨ Step 1. DAG 參數設定
# -------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

# -------------------------------------
# ✨ Step 2. 建立 DAG
# -------------------------------------
with DAG(
    dag_id="d_02-2_etl_hospital_dag",
    description="Hospital ETL pipeline (single-task wrapper)",
    schedule_interval="@monthly",  # 每月執行
    start_date=datetime.now(),
    catchup=False,
    default_args=default_args,
    tags=["hospital", "etl", "arthur", "monthly", "google_API"],
) as dag:

    # -------------------------------------
    # ✨ Step 3. 定義 Airflow 任務
    # -------------------------------------
    extract = PythonOperator(task_id="extract", python_callable=E_hospital.main)
    t_c_d = PythonOperator(
        task_id="get_city_district", python_callable=T_hospital_c_d.main
    )
    t_place_id = PythonOperator(
        task_id="get_place_id", python_callable=T_hospital_place_id.main
    )
    t_details = PythonOperator(
        task_id="get_details", python_callable=T_hospital_details.main
    )
    t_clean = PythonOperator(
        task_id="clean_sort", python_callable=T_hospital_clean_sort.main
    )
    t_id = PythonOperator(task_id="add_id", python_callable=T_hospital_id.main)
    t_merge = PythonOperator(task_id="merge", python_callable=T_hospital_merge.main)
    t_cat_id = PythonOperator(
        task_id="get_cat_id", python_callable=T_hospital_cat_id.main
    )
    t_sql = PythonOperator(task_id="final", python_callable=T_hospital_sql.main)
    load = PythonOperator(task_id="load", python_callable=L_hospital.main)

    # 定義依賴關係（按順序連接）
    (
        extract
        >> t_c_d
        >> t_place_id
        >> t_details
        >> t_clean
        >> t_id
        >> t_merge
        >> t_cat_id
        >> t_sql
        >> load
    )
        >> load
    )
