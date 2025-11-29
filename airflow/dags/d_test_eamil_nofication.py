from datetime import timedelta, datetime
from airflow.decorators import dag, task

# 設定DAG基本資訊
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["add412@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="d_test_eamil_nofication",
    default_args=default_args,
    description="用於測試失敗通知機制",
    schedule_interval="0 8 */1 * *",
    start_date=datetime(2025, 11, 1),
    catchup=False,
    # Optional: Add tags for better filtering in the UI
    tags=["test"]
)
def d_test_eamil_nofication():
    @task
    def failure_trigger():
        x = "hello, world"

        return (x - 2)

    run = failure_trigger()


# d_test_eamil_nofication()
