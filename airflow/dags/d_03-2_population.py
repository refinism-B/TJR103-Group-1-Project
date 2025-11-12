import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# === 🧩 設定模組搜尋路徑 (for tasks/population modules) ===
current_dir = os.path.dirname(os.path.abspath(__file__))          # /opt/airflow/dags
project_root = os.path.dirname(current_dir)                       # /opt/airflow
sys.path.append(os.path.join(project_root, "tasks", "population"))  # /opt/airflow/tasks/population

# === 🧩 匯入自訂模組 ===
# 確認你有 tasks/population/E_pop.py 並包含必要的函式
from E_pop import fetch_raw_data
from T_pop import transform
from L_pop import load

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
    dag_id='d_03-2_population',
    default_args=default_args,
    description='ETL pipeline for Taiwan population data',
    schedule=None,  # Airflow 3.x 新寫法
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['517', 'population', 'monthly'],
) as dag:

    # === 🧮 Extract 任務 ===
    def extract_task():
        print("📊 [E] Extract - 抓取內政部人口統計資料中...")
        df_raw = fetch_raw_data()
        raw_dir = "/opt/airflow/data/raw"
        os.makedirs(raw_dir, exist_ok=True)
        df_raw.to_csv(f"{raw_dir}/population_raw.csv", index=False)
        print(f"✅ Extract 完成，共 {len(df_raw)} 筆資料！")
        return "extract done"

    # === 🧹 Transform 任務 ===
    def transform_task():
        print("⚙️ [T] Transform - 整理人口統計資料中...")
        import pandas as pd
        raw_path = "/opt/airflow/data/raw/population_raw.csv"
        processed_dir = "/opt/airflow/data/processed"
        os.makedirs(processed_dir, exist_ok=True)

        df_raw = pd.read_csv(raw_path)
        df_processed = transform(df_raw)
        df_processed.to_csv(f"{processed_dir}/population_processed.csv", index=False)

        print(f"✅ Transform 完成，輸出 {len(df_processed)} 筆資料！")
        return "transform done"

    # === 💾 Load 任務 ===
    def load_task():
        print("💾 [L] Load - 匯入 MySQL 中...")
        import pandas as pd
        processed_path = "/opt/airflow/data/processed/population_processed.csv"
        df_processed = pd.read_csv(processed_path)
        load(df_processed)
        print("🎉 ETL Population Pipeline 全部完成！")
        return "load done"

    # === DAG 任務順序 ===
    t1 = PythonOperator(task_id='extract', python_callable=extract_task)
    t2 = PythonOperator(task_id='transform', python_callable=transform_task)
    t3 = PythonOperator(task_id='load', python_callable=load_task)

    t1 >> t2 >> t3
