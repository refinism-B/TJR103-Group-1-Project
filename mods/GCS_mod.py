import os

from dotenv import load_dotenv
from google.cloud import storage
from google.oauth2 import service_account


def L_upload_to_gcs(gcs_setting: dict):

    load_dotenv()

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
