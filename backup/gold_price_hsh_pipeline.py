from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import csv
import os
import psycopg2

def collect_data():
    # กำหนด path สำหรับไฟล์ใน project ของ Airflow
    base_path = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'files')
    os.makedirs(base_path, exist_ok=True)
    file_path = os.path.join(base_path, 'gold_hsh_prices.csv')

    url = "https://apicheckprice.huasengheng.com/api/values/getprice/"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        # print(data)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return

    # กรองข้อมูลเฉพาะ GoldType "HSH" และ "REF"
    gold_prices = [item for item in data if item.get('GoldType') in ['HSH', 'REF']]

    if not gold_prices:
        print("No relevant gold prices found.")
        return

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # ตรวจสอบว่ามีไฟล์อยู่แล้วหรือไม่
    file_exists = os.path.isfile(file_path)

    # เก็บข้อมูลลงในไฟล์ CSV
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['timestamp', 'gold_type', 'sell_price', 'buy_price'])
        for price in gold_prices:

            writer.writerow([
                timestamp, 
                price.get('GoldType'), 
                float(price.get('Sell', '0').replace(',', '')), 
                float(price.get('Buy', '0').replace(',', ''))
            ])

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
            CREATE TABLE IF NOT EXISTS gold_hsh_prices (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                gold_type VARCHAR(10) NOT NULL,
                sell_price INTEGER NOT NULL,
                buy_price INTEGER NOT NULL
            )
        ''')
        conn.commit()

        for price in gold_prices:
            # ตรวจสอบว่าข้อมูลนี้มีอยู่แล้วหรือไม่
            cursor.execute(
                """
                SELECT 1 FROM gold_hsh_prices 
                WHERE timestamp = %s AND gold_type = %s AND sell_price = %s AND buy_price = %s
                """,
                (timestamp, price.get('GoldType'), float(price.get('Sell', '0').replace(',', '')), float(price.get('Buy', '0').replace(',', '')))
            )
            result = cursor.fetchone()

            # หากไม่มีข้อมูลนี้ ให้เพิ่มเข้าไป
            if not result:
                cursor.execute(
                    """
                    INSERT INTO gold_hsh_prices (timestamp, gold_type, sell_price, buy_price) 
                    VALUES (%s, %s, %s, %s)
                    """,
                    (timestamp, price.get('GoldType'), float(price.get('Sell', '0').replace(',', '')), float(price.get('Buy', '0').replace(',', '')))
                )
        conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")

def display_data():
    # กำหนด path สำหรับไฟล์ใน project ของ Airflow
    base_path = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'files')
    file_path = os.path.join(base_path, 'gold_hsh_prices.csv')

    # อ่านข้อมูลจากไฟล์ CSV
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        data = list(reader)

    print("\nราคาทองคำ:")
    for row in data:
        print(', '.join(row))

# สร้าง DAG
with DAG(
    'gold_price_hsh_pipeline',
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
    tags=['gold_price_hsh']
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
