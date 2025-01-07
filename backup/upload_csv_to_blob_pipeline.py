from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import os

# ฟังก์ชันสำหรับอัปโหลดไฟล์
def upload_csv_to_blob():
    # ดึง Connection จาก Airflow Connections
    connection = BaseHook.get_connection("azure_blob_storage")
    connection_string = connection.extra_dejson.get("connection_string")
    if not connection_string:
        raise ValueError("Azure connection string not found in Airflow connection settings.")

    # ตั้งค่า Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "csv-container"  # ชื่อ Container ใน Azure Blob Storage
    blob_name = "gold_hsh_prices.csv"     # ชื่อไฟล์ใน Blob Storage

    # ระบุ Path ไฟล์ CSV ใน Local
    local_path = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'files')
    file_path = os.path.join(local_path, 'gold_hsh_prices.csv')

    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    # อัปโหลดไฟล์ไปยัง Azure Blob Storage
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded {file_path} to container {container_name} as {blob_name}.")

# สร้าง DAG
with DAG(
    'upload_csv_to_blob_pipeline',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1
    },
    description='Pipeline สำหรับอัปโหลด CSV ไปยัง Azure Blob Storage',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=True,
    tags=['azure', 'blob', 'upload']
) as dag:

    upload_task = PythonOperator(
        task_id='upload_csv_to_blob',
        python_callable=upload_csv_to_blob
    )

    upload_task
