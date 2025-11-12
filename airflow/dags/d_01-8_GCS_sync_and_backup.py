from datetime import datetime, timedelta
from airflow.decorators import dag, task


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
    dag_id="d_01-8_GCS_sync_and_backup",
    default_args=default_args,
    description="[每日更新]備份並同步最新檔案至GCS",
    schedule_interval="0 0 */30 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "daily", "GCS"]
)
def d_01_8_GCS_sync_and_backup():
