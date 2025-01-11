from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests
import json
import csv
from airflow.hooks.base_hook import BaseHook
from azure.storage.blob import BlobServiceClient
from airflow.models import Variable

# Configuration
TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
DAY = datetime.now().day
MONTH = datetime.now().month
YEAR = datetime.now().year
HOUR = datetime.now().hour
MINUTE = datetime.now().minute
SECOND = datetime.now().second

# Function to fetch data from API
def fetch_api_gold_price(**kwargs):
    url = "https://apicheckprice.huasengheng.com/api/values/getprice/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data:
            print("No data found from API.")
            return None

        file_name = f"gold_hsh_price_{TIMESTAMP}.json"

        local_dir = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'raw_json_files')
        os.makedirs(local_dir, exist_ok=True)
        file_path = os.path.join(local_dir, file_name)

        # Save to JSON file locally
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)
            
        # Save file_path to Airflow Variable
        Variable.set("latest_json_file_path", file_path)

        print(f"Saved data locally to {file_path}")
        return file_path

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# Function to upload JSON file to Azure Blob Storage
def upload_json_to_blob(**context):
    # ดึง Connection จาก Airflow Connections
    connection = BaseHook.get_connection("azure_blob_storage")
    connection_string = connection.extra_dejson.get("connection_string")
    if not connection_string:
        raise ValueError("Azure connection string not found in Airflow connection settings.")

    # ตั้งค่า Blob Service Client
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "rawdata-hsh-json"  # ชื่อ Container ใน Azure Blob Storage

    file_path = context['ti'].xcom_pull(task_ids='fetch_api_gold_price')  # Pull file_path from XCom

    if not file_path or not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return

    # ใช้ชื่อไฟล์เดียวกันกับไฟล์ JSON ใน Blob Storage
    blob_name = os.path.basename(file_path)

    # อัปโหลดไฟล์ไปยัง Azure Blob Storage
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    print(f"Uploaded {file_path} to container {container_name} as {blob_name}.")


with DAG(
    'fetch_and_upload_gold_data',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Pipeline to fetch gold price and upload to Azure Blob Storage',
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['gold_price']
) as dag:

    fetch_api_task = PythonOperator(
        task_id='fetch_api_gold_price',
        python_callable=fetch_api_gold_price,
        provide_context=True
    )

    upload_json_to_blob_task = PythonOperator(
        task_id='upload_json_to_blob',
        python_callable=upload_json_to_blob,
        provide_context=True
    )


    fetch_api_task >> upload_json_to_blob_task 

