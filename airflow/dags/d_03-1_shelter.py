import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# === 🧩 設定模組搜尋路徑 (for tasks/shelter modules) ===
current_dir = os.path.dirname(os.path.abspath(__file__))          # /opt/airflow/dags
project_root = os.path.dirname(current_dir)                       # /opt/airflow
sys.path.append(os.path.join(project_root, "tasks", "shelter"))   # /opt/airflow/tasks/shelter

# === 🧩 匯入自訂模組 ===
from E_shelter import fetch_raw_data
from T_shelter import transform
from L_shelter import load

# === ⚙️ DAG 預設參數 ===
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# === 🗓️ 定義 DAG ===
with DAG(
    dag_id='d_03-1_shelter',
    default_args=default_args,
    description='ETL pipeline for Taiwan shelter data',
    schedule=None,  # Airflow 3.x 新參數（取代 schedule_interval）
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['517', 'shelter', 'monthly'],
) as dag:

    # === 🐾 Extract 任務 ===
    def extract_task():
        print("🐾 [E] Extract - 抓取農業部資料中...")
        df_raw = fetch_raw_data()
        raw_dir = "/opt/airflow/data/raw"
        os.makedirs(raw_dir, exist_ok=True)
        df_raw.to_csv(f"{raw_dir}/shelter_raw.csv", index=False)
        print(f"✅ Extract 完成，共 {len(df_raw)} 筆資料！")
        return "extract done"

    # === ⚙️ Transform 任務 ===
    def transform_task():
        print("⚙️ [T] Transform - 清理與 Google 資料整合中...")
        import pandas as pd
        raw_path = "/opt/airflow/data/raw/shelter_raw.csv"
        processed_dir = "/opt/airflow/data/processed"
        os.makedirs(processed_dir, exist_ok=True)

        df_raw = pd.read_csv(raw_path)
        df_processed = transform(df_raw)
        df_processed.to_csv(f"{processed_dir}/shelter_processed.csv", index=False)

        print(f"✅ Transform 完成，輸出 {len(df_processed)} 筆資料！")
        return "transform done"

    # === 💾 Load 任務 ===
    def load_task():
        print("💾 [L] Load - 匯入 MySQL 中...")
        import pandas as pd
        processed_path = "/opt/airflow/data/processed/shelter_processed.csv"
        df_processed = pd.read_csv(processed_path)
        load(df_processed)
        print("🎉 ETL Shelter Pipeline 全部完成！")
        return "load done"

    # === 定義三個任務 ===
    t1 = PythonOperator(task_id='extract', python_callable=extract_task)
    t2 = PythonOperator(task_id='transform', python_callable=transform_task)
    t3 = PythonOperator(task_id='load', python_callable=load_task)

    # === DAG 流程順序 ===
    t1 >> t2 >> t3
