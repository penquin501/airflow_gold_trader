from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import requests
from airflow.models import Variable

# ฟังก์ชันสำหรับดึงข้อมูลราคาล่าสุดและก่อนหน้าจาก PostgreSQL
def get_latest_and_previous_gold_prices():
    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
        cursor = conn.cursor()

        # ดึงข้อมูลราคาล่าสุดและก่อนหน้า
        cursor.execute("""
            SELECT timestamp, sell_price, buy_price
            FROM gold_prices
            ORDER BY id DESC
            LIMIT 2;
        """)
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        if results and len(results) > 1:
            current_price = {
                'timestamp': results[0][0],
                'sell_price': results[0][1],
                'buy_price': results[0][2]
            }
            previous_price = {
                'timestamp': results[1][0],
                'sell_price': results[1][1],
                'buy_price': results[1][2]
            }
            return current_price, previous_price
        elif results:
            current_price = {
                'timestamp': results[0][0],
                'sell_price': results[0][1],
                'buy_price': results[0][2]
            }
            return current_price, None
        else:
            return None, None
    except Exception as e:
        print(f"Error retrieving gold prices from PostgreSQL: {e}")
        return None, None

# ฟังก์ชันสำหรับแจ้งเตือน LINE Notify เมื่อราคามีการเปลี่ยนแปลง
def notify_price_change(**context):
    prices = context['ti'].xcom_pull(task_ids='get_latest_and_previous_gold_prices')

    if not prices or len(prices) != 2:
        print("Insufficient price data found.")
        return

    current_price, previous_price = prices

    if not previous_price:
        print("No previous price to compare. Notification skipped.")
        return

    if (current_price['sell_price'] != previous_price['sell_price'] or
            current_price['buy_price'] != previous_price['buy_price']):

        line_token = Variable.get("LINE_NOTIFY_TOKEN")
        if not line_token:
            print("LINE_NOTIFY_TOKEN not found.")
            return

        headers = {
            "Authorization": f"Bearer {line_token}"
        }
        message = (
            f"\n\u2B50 ราคาทองคำมีการเปลี่ยนแปลง \u2B50\n"
            f"- ราคาก่อนหน้า\n"
            f"  - ขายออก: {previous_price['sell_price']} บาท\n"
            f"  - รับซื้อ: {previous_price['buy_price']} บาท\n"
            f"- ราคาปัจจุบัน\n"
            f"  - ขายออก: {current_price['sell_price']} บาท\n"
            f"  - รับซื้อ: {current_price['buy_price']} บาท\n"
        )

        try:
            response = requests.post(
                url="https://notify-api.line.me/api/notify",
                headers=headers,
                data={"message": message}
            )

            if response.status_code == 200:
                print("Notification sent successfully.")
            else:
                print(f"Failed to send notification: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Error sending LINE notification: {e}")
    else:
        print("No price change detected. No notification sent.")


# สร้าง DAG
with DAG(
    'notify_price_change_pipeline',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1
    },
    description='Pipeline สำหรับแจ้งเตือนเมื่อราคาทองคำมีการเปลี่ยนแปลง',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gold_price', 'line_notify']
) as dag:

    get_latest_prices_task = PythonOperator(
        task_id='get_latest_and_previous_gold_prices',
        python_callable=get_latest_and_previous_gold_prices
    )

    notify_task = PythonOperator(
        task_id='notify_price_change',
        python_callable=notify_price_change,
        provide_context=True
    )

    get_latest_prices_task >> notify_task
