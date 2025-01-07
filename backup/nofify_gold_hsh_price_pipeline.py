from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import requests
from airflow.models import Variable

# Configuration for dates and times
TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
DAY = datetime.now().day
MONTH = datetime.now().month
YEAR = datetime.now().year
HOUR = datetime.now().hour
MINUTE = datetime.now().minute
SECOND = datetime.now().second

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
            SELECT timestamp, gold_type, sell_price, buy_price
            FROM gold_hsh_prices
            WHERE gold_type='HSH'
            ORDER BY id DESC
            LIMIT 2;
        """)
        results = cursor.fetchall()

        cursor.close()
        conn.close()

        if results and len(results) > 1:
            current_price = {
                'timestamp': results[0][0],
                'gold_type': results[0][1],
                'sell_price': results[0][2],
                'buy_price': results[0][3]
            }
            previous_price = {
                'timestamp': results[1][0],
                'gold_type': results[1][1],
                'sell_price': results[1][2],
                'buy_price': results[1][3]
            }
            return current_price, previous_price
        elif results:
            current_price = {
                'timestamp': results[0][0],
                'gold_type': results[0][1],
                'sell_price': results[0][2],
                'buy_price': results[0][3]
            }
            return current_price, None
        else:
            return None, None
    except Exception as e:
        print(f"Error retrieving gold hsh prices from PostgreSQL: {e}")
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

        personal_token = Variable.get("LINE_NOTIFY_PERSONAL_TOKEN", default_var=None)
        group_token = Variable.get("LINE_NOTIFY_GROUP_TOKEN", default_var=None)

        if not personal_token and not group_token:
            print("No LINE_NOTIFY_TOKENs found for both personal and group notifications.")
            return

        datetime = f"{DAY}/{MONTH}/{YEAR} {HOUR}:{MINUTE}" # localtime
        message = (
            f"\n⭐ ราคาทองคำของ hsh มีการเปลี่ยนแปลง {datetime} ⭐\n"
            f"- ราคาก่อนหน้า\n"
            f"  - ขายออก: {previous_price['sell_price']} บาท\n"
            f"  - รับซื้อ: {previous_price['buy_price']} บาท\n"
            f"- ราคาปัจจุบัน\n"
            f"  - ขายออก: {current_price['sell_price']} บาท\n"
            f"  - รับซื้อ: {current_price['buy_price']} บาท\n"
        )

        def send_line_notification(token, message):
            headers = {
                "Authorization": f"Bearer {token}"
            }
            try:
                response = requests.post(
                    url="https://notify-api.line.me/api/notify",
                    headers=headers,
                    data={"message": message}
                )

                if response.status_code == 200:
                    print(f"Notification sent successfully to token: {token[:5]}...")
                else:
                    print(f"Failed to send notification to token {token[:5]}...: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"Error sending LINE notification to token {token[:5]}...: {e}")

        if personal_token:
            print("Sending notification to personal LINE.")
            send_line_notification(personal_token, message)

        if group_token:
            print("Sending notification to group LINE.")
            send_line_notification(group_token, message)

    else:
        print("No price change detected. No notification sent.")


# สร้าง DAG
with DAG(
    'nofify_gold_hsh_price_pipeline',
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
    tags=['gold_hsh_price', 'line_notify']
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
