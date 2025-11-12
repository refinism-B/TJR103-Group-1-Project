from datetime import datetime, timedelta, date
from airflow.decorators import dag, task
from tasks import GCS_mod as gcs


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
    description="[每日更新]備份GCS中的完成檔案",
    schedule_interval="0 0 10 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "monthly", "GCS", "backup"]
)
def d_01_8_GCS_sync_and_backup():
    @task
    def S_get_backup_setting():
        """取得備份所需的路徑資訊"""
        file_date = date.today().strftime("%Y%m%d")
        bucket_name = "tjr103-1-project-bucket"
        source_folder = "data/complete/store/"
        destination_folder = f"data/backup/{file_date}_store/"

        return {
            "bucket_name": bucket_name,
            "source_folder": source_folder,
            "destination_folder": destination_folder
        }

    backup_setting = S_get_backup_setting()

    gcs.T_backup_file(backup_setting=backup_setting)


d_01_8_GCS_sync_and_backup()
