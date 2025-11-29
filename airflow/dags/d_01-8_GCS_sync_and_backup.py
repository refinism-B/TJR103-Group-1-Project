from datetime import datetime, timedelta, date
from airflow.decorators import dag, task
from tasks import GCS_mod as gcs


# 設定DAG基本資訊
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["add412@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="d_01-8_GCS_sync_and_backup",
    default_args=default_args,
    description="[每月更新]備份GCS中的完成檔案",
    schedule_interval="0 10 20 * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["bevis", "monthly", "GCS", "backup", "20/10:00"]
)
def d_01_8_GCS_sync_and_backup():
    @task
    def S_get_backup_setting():
        """取得備份所需的路徑資訊"""
        file_date = date.today().strftime("%Y%m%d")
        bucket_name = "tjr103-1-project-bucket"
        source_folder = "data/"
        destination_folder = f"backup/{file_date}_data/"

        return {
            "bucket_name": bucket_name,
            "source_folder": source_folder,
            "destination_folder": destination_folder
        }

    @task
    def S_get_upload_folder_setting():
        """取得目標GCS的路徑資訊"""
        source_folder = "/opt/airflow/data"
        destination_folder = "data"
        bucket_name = "tjr103-1-project-bucket"

        return {
            "source_folder": source_folder,
            "destination_folder": destination_folder,
            "bucket_name": bucket_name
        }

    backup_setting = S_get_backup_setting()

    backup = gcs.T_backup_file(backup_setting=backup_setting)

    folder_setting = S_get_upload_folder_setting()

    gcs.L_upload_folder_to_gcs(folder_setting=folder_setting)

    backup >> folder_setting


d_01_8_GCS_sync_and_backup()
