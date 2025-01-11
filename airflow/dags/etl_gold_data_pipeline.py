from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import csv
import json
import psycopg2
from airflow.hooks.base import BaseHook
from airflow.models import Variable

# Configuration for dates and times
TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
DAY = datetime.now().day
MONTH = datetime.now().month
YEAR = datetime.now().year
HOUR = datetime.now().hour
MINUTE = datetime.now().minute
SECOND = datetime.now().second

# Extract function
def convert_json_to_csv(**kwargs):
    # Retrieve file_path from Airflow Variable
    json_file_path = Variable.get("latest_json_file_path", default_var=None)
    if not json_file_path:
        print(f"JSON file {json_file_path} not found.")
        return None

    try:
        local_csv_directory = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'files', 'raw_csv')
        os.makedirs(local_csv_directory, exist_ok=True)

        raw_csv_path = os.path.join(local_csv_directory, "gold_prices_raw.csv")
        with open(json_file_path, "r") as json_file:
            data = json.load(json_file)

        # ตรวจสอบว่าไฟล์ CSV มีข้อมูลหรือไม่
        file_exists = os.path.isfile(raw_csv_path)
        write_header = not file_exists or os.stat(raw_csv_path).st_size == 0  # เขียน header หากไฟล์ไม่มีข้อมูล

        with open(raw_csv_path, "a", newline="") as raw_csv:
            writer = csv.DictWriter(raw_csv, fieldnames=["GoldType", "Buy", "Sell", "Timestamp"])

            # เขียน header ถ้าไฟล์ยังไม่มีข้อมูล
            if write_header:
                writer.writeheader()

            for item in data:
                writer.writerow({
                    "GoldType": item.get("GoldType"),
                    "Buy": item.get("Buy"),
                    "Sell": item.get("Sell"),
                    "Timestamp": TIMESTAMP
                })

        print(f"Saved raw data to {raw_csv_path}")

        # Push CSV path to XCom
        kwargs['ti'].xcom_push(key='raw_csv_path', value=raw_csv_path)

        return raw_csv_path
    except Exception as e:
        print(f"Error: {e}")
        return None

# Transform Task
def transform_csv(**kwargs):
    raw_csv_path = kwargs['ti'].xcom_pull(task_ids='extract', key='raw_csv_path')
    if not raw_csv_path:
        print(f"Raw CSV file {raw_csv_path} not found.")
        return None

    try:
        # เตรียม Directory ปลายทางที่เราจะเก็บไฟล์ HSH และ REF
        local_csv_directory = os.path.join(
            os.getenv('AIRFLOW_HOME', '/opt/airflow'),
            'dags', 'files', 'cleaned_csv'
        )
        os.makedirs(local_csv_directory, exist_ok=True)

        # ตั้งชื่อไฟล์ผลลัพธ์
        hsh_csv_path = os.path.join(local_csv_directory, "gold_hsh_prices.csv")
        ref_csv_path = os.path.join(local_csv_directory, "gold_ref_prices.csv")

        # อ่านข้อมูลจาก raw CSV พร้อมตรวจสอบ header
        with open(raw_csv_path, "r", encoding="utf-8") as raw_csv:
            reader = csv.DictReader(raw_csv)
            headers = reader.fieldnames
            print("Headers:", headers)  # Debug: ตรวจสอบ header

            # อ่านข้อมูลทั้งหมด
            new_data = [row for row in reader]
            print("New Data:", new_data)  # Debug: ตรวจสอบข้อมูลใหม่

        # ฟังก์ชันอ่าน Timestamp ที่มีอยู่ในไฟล์ CSV ปลายทาง
        def read_existing_timestamps(file_path):
            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                return set()
            with open(file_path, "r", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)
                return {row["Timestamp"] for row in reader}

        # อ่าน Timestamp ที่มีอยู่แล้วในไฟล์ HSH และ REF
        existing_hsh_timestamps = read_existing_timestamps(hsh_csv_path)
        existing_ref_timestamps = read_existing_timestamps(ref_csv_path)

        print("Existing HSH Timestamps:", existing_hsh_timestamps)  # Debug
        print("Existing REF Timestamps:", existing_ref_timestamps)  # Debug

        # สร้าง fieldnames ที่ต้องการเก็บในไฟล์ CSV ปลายทาง
        fieldnames = ["GoldType", "Buy", "Sell", "Timestamp", "Day", "Month", "Year", "Hour", "Minute", "Second"]

        # แยกข้อมูล HSH และ REF พร้อมกรองข้อมูลซ้ำ
        hsh_data = [
            row for row in new_data if row.get("GoldType") == "HSH" and row.get("Timestamp") not in existing_hsh_timestamps
        ]
        ref_data = [
            row for row in new_data if row.get("GoldType") == "REF" and row.get("Timestamp") not in existing_ref_timestamps
        ]

        print("Filtered HSH Data:", hsh_data)  # Debug: ตรวจสอบข้อมูล HSH ที่ไม่ซ้ำ
        print("Filtered REF Data:", ref_data)  # Debug: ตรวจสอบข้อมูล REF ที่ไม่ซ้ำ

        # ฟังก์ชันเขียนข้อมูลใหม่ลงในไฟล์ CSV
        def write_to_csv(data, file_path):
            with open(file_path, "a", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                # เช็คว่าขนาดไฟล์เป็น 0 หรือไม่ (ถ้าไฟล์ยังไม่มี header)
                if os.path.getsize(file_path) == 0:
                    writer.writeheader()

                for row in data:
                    try:
                        # แปลง Timestamp จาก row.get("Timestamp")
                        raw_timestamp = row.get("Timestamp", "").replace("-", "").replace(" ", "").replace(":", "")
                        timestamp = datetime.strptime(raw_timestamp, "%Y%m%d%H%M%S")

                        writer.writerow({
                            "GoldType": row.get("GoldType"),
                            "Buy": float(row.get("Buy").replace(",", "")),
                            "Sell": float(row.get("Sell").replace(",", "")),
                            "Timestamp": raw_timestamp,  # ใช้ Timestamp ในรูปแบบ 20250111162030
                            "Day": timestamp.day,
                            "Month": timestamp.month,
                            "Year": timestamp.year,
                            "Hour": timestamp.hour,
                            "Minute": timestamp.minute,
                            "Second": timestamp.second
                        })
                    except Exception as e:
                        print(f"Error processing row: {row}, Error: {e}")

        # บันทึกข้อมูลลงไฟล์ HSH และ REF
        write_to_csv(hsh_data, hsh_csv_path)
        write_to_csv(ref_data, ref_csv_path)

        # push XCom เก็บ path ของไฟล์ผลลัพธ์
        kwargs['ti'].xcom_push(key='hsh_csv_path', value=hsh_csv_path)
        kwargs['ti'].xcom_push(key='ref_csv_path', value=ref_csv_path)

        print(f"Appended transformed data to {hsh_csv_path} and {ref_csv_path}")

    except Exception as e:
        print(f"Error transforming CSV: {e}")

# Load Task
def load_csv_to_postgres(**kwargs):
    hsh_csv_path = kwargs['ti'].xcom_pull(task_ids='transform', key='hsh_csv_path')
    ref_csv_path = kwargs['ti'].xcom_pull(task_ids='transform', key='ref_csv_path')

    try:
        connection = BaseHook.get_connection("postgres_default")
        conn = psycopg2.connect(
            dbname=connection.schema,
            user=connection.login,
            password=connection.password,
            host=connection.host,
            port=connection.port
        )
        cursor = conn.cursor()

        for csv_path, table_name in [(hsh_csv_path, "hsh_prices"), (ref_csv_path, "ref_prices")]:
            if not csv_path or not os.path.exists(csv_path):
                print(f"CSV file {csv_path} not found for table {table_name}")
                continue

            # Ensure table exists
            ensure_table_exists(cursor, table_name)

            # Load data into the table
            with open(csv_path, "r", encoding="utf-8") as file:
                cursor.copy_expert(f"""
                    COPY {table_name}(goldtype, buy, sell, timestamp, day, month, year, hour, minute, second)
                    FROM STDIN WITH CSV HEADER
                """, file)

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error loading data to Postgres: {e}")

def ensure_table_exists(cursor, table_name):
    """Check if a table exists in the database and create it if not."""
    create_table_queries = {
        "hsh_prices": """
            CREATE TABLE IF NOT EXISTS hsh_prices (
                id SERIAL PRIMARY KEY,
                GoldType VARCHAR(10),
                Buy FLOAT,
                Sell FLOAT,
                Timestamp VARCHAR(20),
                Day INT,
                Month INT,
                Year INT,
                Hour INT,
                Minute INT,
                Second INT
            );
            CREATE INDEX IF NOT EXISTS idx_hsh_prices_timestamp ON hsh_prices (Timestamp);
            CREATE INDEX IF NOT EXISTS idx_hsh_prices_sell ON hsh_prices (Sell);
        """,
        "ref_prices": """
            CREATE TABLE IF NOT EXISTS ref_prices (
                id SERIAL PRIMARY KEY,
                GoldType VARCHAR(10),
                Buy FLOAT,
                Sell FLOAT,
                Timestamp VARCHAR(20),
                Day INT,
                Month INT,
                Year INT,
                Hour INT,
                Minute INT,
                Second INT
            );
            CREATE INDEX IF NOT EXISTS idx_ref_prices_timestamp ON ref_prices (Timestamp);
            CREATE INDEX IF NOT EXISTS idx_ref_prices_sell ON ref_prices (Sell);
        """
    }

    if table_name in create_table_queries:
        cursor.execute(create_table_queries[table_name])
        print(f"Ensured table {table_name} exists.")
    else:
        raise ValueError(f"No create table query defined for table: {table_name}")



# DAG definition
default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'etl_gold_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for gold price data',
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'gold_price']
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=convert_json_to_csv,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_csv,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_csv_to_postgres,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
