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

# Function to convert JSON to CSV
# def convert_json_to_csv(json_file_path):
#     if not os.path.exists(json_file_path):
#         print(f"JSON file {json_file_path} not found.")
#         return None

#     try:
#         with open(json_file_path, "r") as json_file:
#             data = json.load(json_file)

#         if not data:
#             print("No data to convert.")
#             return None

        # hsh_data = [item for item in data if item.get("GoldType") == "HSH"]
        # ref_data = [item for item in data if item.get("GoldType") == "REF"]

        # # Ensure local directory for CSV exists
        # local_csv_directory = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'csv_files')
        # os.makedirs(local_csv_directory, exist_ok=True)

        # hsh_csv_path = os.path.join(local_csv_directory, "gold_hsh_prices.csv")
        # ref_csv_path = os.path.join(local_csv_directory, "gold_ref_prices.csv")

        # fieldnames = ["GoldType", "Buy", "Sell", "Timestamp", "Day", "Month", "Year", "Hour", "Minute", "Second"]
        
        # # Append HSH data to CSV
        # hsh_file_exists = os.path.isfile(hsh_csv_path)
        # with open(hsh_csv_path, "a", newline="") as hsh_csv:
        #     hsh_writer = csv.DictWriter(hsh_csv, fieldnames=fieldnames)
        #     if not hsh_file_exists:
        #         hsh_writer.writeheader()
        #     for item in hsh_data:
        #         hsh_writer.writerow({
        #             "GoldType": item.get("GoldType"),
        #             "Buy": float(item.get("Buy", "0").replace(",", "")),
        #             "Sell": float(item.get("Sell", "0").replace(",", "")),
        #             "Timestamp": TIMESTAMP,
        #             "Day": DAY,
        #             "Month": MONTH,
        #             "Year": YEAR,
        #             "Hour": HOUR,
        #             "Minute": MINUTE,
        #             "Second": SECOND
        #         })

        # # Append REF data to CSV
        # ref_file_exists = os.path.isfile(ref_csv_path)
        # with open(ref_csv_path, "a", newline="") as ref_csv:
        #     ref_writer = csv.DictWriter(ref_csv, fieldnames=fieldnames)
        #     if not ref_file_exists:
        #         ref_writer.writeheader()
        #     for item in ref_data:
        #         ref_writer.writerow({
        #             "GoldType": item.get("GoldType"),
        #             "Buy": float(item.get("Buy", "0").replace(",", "")),
        #             "Sell": float(item.get("Sell", "0").replace(",", "")),
        #             "Timestamp": TIMESTAMP,
        #             "Day": DAY,
        #             "Month": MONTH,
        #             "Year": YEAR,
        #             "Hour": HOUR,
        #             "Minute": MINUTE,
        #             "Second": SECOND
        #         })

        # print(f"Saved HSH data to {hsh_csv_path} and REF data to {ref_csv_path}")
        # return hsh_csv_path, ref_csv_path

#     except Exception as e:
#         print(f"Error converting JSON to CSV: {e}")
#         return None

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

