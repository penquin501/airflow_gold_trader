from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import csv
import os
import psycopg2

def collect_data():
    # กำหนด path สำหรับไฟล์ใน project ของ Airflow
    base_path = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'files')
    os.makedirs(base_path, exist_ok=True)
    file_path = os.path.join(base_path, 'gold_prices.csv')

    url = "https://www.goldtraders.or.th/"

    # สร้าง Session พร้อม Retry
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')

    # ดึงข้อมูลราคาทองคำแท่ง 96.5% ขายออกและรับซื้อ
    try:
        sell_price = int(float(soup.find('span', id='DetailPlace_uc_goldprices1_lblBLSell').text.strip().replace(',', '')))
        buy_price = int(float(soup.find('span', id='DetailPlace_uc_goldprices1_lblBLBuy').text.strip().replace(',', '')))
    except AttributeError as e:
        print(f"Error parsing HTML: {e}")
        return

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # ตรวจสอบว่ามีไฟล์อยู่แล้วหรือไม่
    file_exists = os.path.isfile(file_path)

    # เก็บข้อมูลลงในไฟล์ CSV
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['timestamp', 'sell_price', 'buy_price'])
        writer.writerow([timestamp, sell_price, buy_price])

    # บันทึกข้อมูลลงใน PostgreSQL
    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
        cursor = conn.cursor()

        # สร้างตารางถ้ายังไม่มี
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS gold_prices (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                sell_price INTEGER NOT NULL,
                buy_price INTEGER NOT NULL
            )
        ''')
        conn.commit()

        # ตรวจสอบว่าข้อมูลนี้มีอยู่แล้วหรือไม่
        cursor.execute(
            "SELECT 1 FROM gold_prices WHERE timestamp = %s AND sell_price = %s AND buy_price = %s", 
            (timestamp, sell_price, buy_price)
        )
        result = cursor.fetchone()

        # หากไม่มีข้อมูลนี้ ให้เพิ่มเข้าไป
        if not result:
            cursor.execute(
                "INSERT INTO gold_prices (timestamp, sell_price, buy_price) VALUES (%s, %s, %s)",
                (timestamp, sell_price, buy_price)
            )
            conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")

def display_data():
    # กำหนด path สำหรับไฟล์ใน project ของ Airflow
    base_path = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'files')
    file_path = os.path.join(base_path, 'gold_prices.csv')

    # อ่านข้อมูลจากไฟล์ CSV
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        data = list(reader)

    print("\nราคาทองคำ 96.5%:")
    for row in data:
        print(', '.join(row))

# สร้าง DAG
with DAG(
    'gold_price_pipeline',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1
    },
    description='Pipeline สำหรับดึงและแสดงผลราคาทองคำ',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gold_price']
) as dag:

    collect_task = PythonOperator(
        task_id='collect_data',
        python_callable=collect_data
    )

    display_task = PythonOperator(
        task_id='display_data',
        python_callable=display_data
    )

    collect_task >> display_task
