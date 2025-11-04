from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# 匯入 ETL 模組
from src.extract import E_hospital
from src.transform import (
    T_hospital_details,
    T_hospital_clean_sort,
    T_hospital_id,
    T_hospital_merge,
    T_hospital_cat_id,
    T_hospital_sql,
)
from src.load import L_hospital
from transform import T_hotel_c_d, T_hotel_place_id

# 預設的參數
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# 設定DAG script的資訊
with DAG(
    dag_id="etl_hospital_dag",
    description="Hospital ETL pipeline (multi-transform)",
    schedule_interval="@month",
    start_date=datetime.now(),
    catchup=False,
    default_args=default_args,
    tags=["hospital", "etl"],
) as dag:

    # Extract
    extract = PythonOperator(
        task_id="extract_hospital",  # 任務名稱
        python_callable=E_hospital.main,  # 要呼叫的.py檔
    )

    # Transform Steps
    t_c_d = PythonOperator(task_id="T_c_d", python_callable=T_hotel_c_d.main)
    t_place = PythonOperator(
        task_id="T_place_id", python_callable=T_hotel_place_id.main
    )
    t_details = PythonOperator(
        task_id="T_details", python_callable=T_hospital_details.main
    )
    t_clean = PythonOperator(
        task_id="T_clean_sort", python_callable=T_hospital_clean_sort.main
    )
    t_id = PythonOperator(task_id="T_id", python_callable=T_hospital_id.main)
    t_merge = PythonOperator(task_id="T_merge", python_callable=T_hospital_merge.main)
    t_cat = PythonOperator(task_id="T_cat_id", python_callable=T_hospital_cat_id.main)
    t_sql = PythonOperator(task_id="T_sql", python_callable=T_hospital_sql.main)

    # Load
    load = PythonOperator(
        task_id="load_hospital",
        python_callable=L_hospital.main,
    )

    # 設定執行順序
    (
        extract
        >> t_c_d
        >> t_place
        >> t_details
        >> t_clean
        >> t_id
        >> t_merge
        >> t_cat
        >> t_sql
        >> load
    )
