import os

from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account
from airflow.decorators import task

load_dotenv()

"""
這個模組主要用於GCS的操作。
通常寫作 import GCS_mod as gcs。
"""


@task
def L_upload_to_gcs(gcs_setting: dict):
    """
    提供"gcs_setting"上傳設定檔，會自動抓取"bucket_name"、"destination"、
    "source_file_name"等資訊，並自動將檔案上傳至GCS。

    注意：請準備好GCS的json key檔案，並將路徑寫入.env。
    """

    credential_path = os.getenv("GCS_KEY_PATH")
    credentials = service_account.Credentials.from_service_account_file(
        credential_path)

    bucket_name = gcs_setting["bucket_name"]
    destination = gcs_setting["destination"]
    source_file_name = gcs_setting["source_file_name"]

    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_filename(source_file_name)

    print(f"{bucket_name}/{destination}上傳成功！")


@task
def T_backup_file(backup_setting: dict):
    """
    主要功能是將GCS上的某一個資料夾中的所有資料夾及檔案，
    複製備份到另一個位置中。

    輸入的backup_setting需要是一個字典，
    其中包含"bucket_name"、"source_folder"、"destination_folder"三項資訊
    才能讓GCS操作檔案

    注意：請準備好GCS的json key檔案，並將路徑寫入.env。
    """
    credential_path = os.getenv("GCS_KEY_PATH")
    credentials = service_account.Credentials.from_service_account_file(
        credential_path)

    bucket_name = backup_setting["bucket_name"]
    source_folder = backup_setting["source_folder"]
    destination_folder = backup_setting["destination_folder"]

    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob_list = bucket.list_blobs(prefix=source_folder)
    for blob in blob_list:
        new_path = blob.name.replace(source_folder, destination_folder, 1)
        new_blob = bucket.copy_blob(
            blob=blob, destination_bucket=bucket, new_name=new_path)

        print(f"已將{blob.name}複製至{new_path}")
