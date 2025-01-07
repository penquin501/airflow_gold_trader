from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime
from psycopg2.extras import DictCursor
from datetime import datetime, timezone, timedelta
import pytz

# Configuration for dates and times
TIMESTAMP = datetime.now().strftime("%Y%m%d%H%M%S")
DAY = datetime.now().day
MONTH = datetime.now().month
YEAR = datetime.now().year
HOUR = datetime.now().hour
MINUTE = datetime.now().minute
SECOND = datetime.now().second

# Utility function to get database connection
def get_postgres_connection():
    connection = BaseHook.get_connection("postgres_default")
    conn = psycopg2.connect(
        dbname=connection.schema,
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port
    )
    return conn

# Utility function to fetch latest and previous gold prices
def fetch_hsh_prices():
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        query = """
            SELECT * FROM hsh_prices ORDER BY timestamp DESC LIMIT 2;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        if len(rows) < 2:
            print("Insufficient price data found.")
            return None, None

        # Convert timestamp to Asia/Bangkok timezone
        current_timestamp = rows[0]['timestamp']
        if isinstance(current_timestamp, str) and len(current_timestamp) == 14:
            # Parse custom timestamp format YYYYMMDDHHMMSS
            current_timestamp = datetime.strptime(current_timestamp, "%Y%m%d%H%M%S")

        if current_timestamp.tzinfo is None:
            # If timestamp is naive, assume it's in UTC
            current_timestamp = current_timestamp.replace(tzinfo=timezone.utc)

        bangkok_tz = pytz.timezone("Asia/Bangkok")
        local_time = current_timestamp.astimezone(bangkok_tz)
        datetime_str = local_time.strftime("%d/%m/%Y %H:%M")

        current_price = {
            "sell_price": float(rows[0]['sell']),
            "buy_price": float(rows[0]['buy']),
            "datetime": datetime_str
        }
        previous_price = {
            "sell_price": float(rows[1]['sell']),
            "buy_price": float(rows[1]['buy'])
        }

        return current_price, previous_price
    except Exception as e:
        print(f"Error fetching prices: {e}")
        return None, None
    
def fetch_ref_prices():
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor(cursor_factory=DictCursor)

        query = """
            SELECT * FROM ref_prices ORDER BY timestamp DESC LIMIT 2;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        if len(rows) < 2:
            print("Insufficient price data found.")
            return None, None
        
        # Convert timestamp to Asia/Bangkok timezone
        current_timestamp = rows[0]['timestamp']
        if isinstance(current_timestamp, str) and len(current_timestamp) == 14:
            # Parse custom timestamp format YYYYMMDDHHMMSS
            current_timestamp = datetime.strptime(current_timestamp, "%Y%m%d%H%M%S")

        if current_timestamp.tzinfo is None:
            # If timestamp is naive, assume it's in UTC
            current_timestamp = current_timestamp.replace(tzinfo=timezone.utc)

        bangkok_tz = pytz.timezone("Asia/Bangkok")
        local_time = current_timestamp.astimezone(bangkok_tz)
        datetime_str = local_time.strftime("%d/%m/%Y %H:%M")

        current_price = {
            "sell_price": float(rows[0]['sell']),
            "buy_price": float(rows[0]['buy']),
            "datetime": datetime_str
        }
        previous_price = {
            "sell_price": float(rows[1]['sell']),
            "buy_price": float(rows[1]['buy'])
        }

        return current_price, previous_price
    except Exception as e:
        print(f"Error fetching prices: {e}")
        return None, None

# Utility function to send LINE notification
def send_line_notification(message, token):
    try:
        headers = {
            "Authorization": f"Bearer {token}"
        }
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

# Notify task
def notify_hsh_price_change(**kwargs):
    current_price, previous_price = fetch_hsh_prices()

    if not current_price or not previous_price:
        print("Skipping notification due to insufficient price data.")
        return

    if (current_price['sell_price'] != previous_price['sell_price'] or
            current_price['buy_price'] != previous_price['buy_price']):

        line_personal_token = Variable.get("LINE_NOTIFY_PERSONAL_TOKEN", default_var=None)
        line_group_token = Variable.get("LINE_NOTIFY_GROUP_TOKEN", default_var=None)

        if not line_personal_token or not line_group_token:
            print("LINE Notify tokens not found in Airflow Variables.")
            return

        # คำนวณส่วนต่างราคา
        diff_sell = current_price['sell_price'] - previous_price['sell_price']
        diff_buy = current_price['buy_price'] - previous_price['buy_price']

        # เลือกอีโมจิสำหรับสถานะ
        sell_status = "🟢📈⬆️" if diff_sell > 0 else "🔴📉⬇️"
        buy_status = "🟢📈⬆️" if diff_buy > 0 else "🔴📉⬇️"

        message_hsh = (
            f"\n🔔 ราคาทองคำของ HSH มีการเปลี่ยนแปลง {current_price['datetime']} 🔔\n"
            f"- ราคาก่อนหน้า\n"
            f"  - ขายออก: {previous_price['sell_price']} บาท\n"
            f"  - รับซื้อ: {previous_price['buy_price']} บาท\n"
            f"- ราคาปัจจุบัน\n"
            f"  - ขายออก: {current_price['sell_price']} บาท {sell_status} {diff_sell} บาท\n"
            f"  - รับซื้อ: {current_price['buy_price']} บาท {buy_status} {diff_buy} บาท\n"
        )

        # Send notifications
        send_line_notification(message_hsh, line_personal_token)
        send_line_notification(message_hsh, line_group_token)

def notify_ref_price_change(**kwargs):
    current_price, previous_price = fetch_ref_prices()

    if not current_price or not previous_price:
        print("Skipping notification due to insufficient price data.")
        return

    if (current_price['sell_price'] != previous_price['sell_price'] or
            current_price['buy_price'] != previous_price['buy_price']):

        line_personal_token = Variable.get("LINE_NOTIFY_PERSONAL_TOKEN", default_var=None)
        line_group_token = Variable.get("LINE_NOTIFY_GROUP_TOKEN", default_var=None)

        if not line_personal_token or not line_group_token:
            print("LINE Notify tokens not found in Airflow Variables.")
            return

        # คำนวณส่วนต่างราคา
        diff_sell = current_price['sell_price'] - previous_price['sell_price']
        diff_buy = current_price['buy_price'] - previous_price['buy_price']

        # เลือกอีโมจิสำหรับสถานะ
        sell_status = "🟢📈⬆️" if diff_sell > 0 else "🔴📉⬇️"
        buy_status = "🟢📈⬆️" if diff_buy > 0 else "🔴📉⬇️"

        message_hsh = (
            f"\n🔔 ราคาทองคำของ สมาคม มีการเปลี่ยนแปลง {current_price['datetime']} 🔔\n"
            f"- ราคาก่อนหน้า\n"
            f"  - ขายออก: {previous_price['sell_price']} บาท\n"
            f"  - รับซื้อ: {previous_price['buy_price']} บาท\n"
            f"- ราคาปัจจุบัน\n"
            f"  - ขายออก: {current_price['sell_price']} บาท {sell_status} {diff_sell} บาท\n"
            f"  - รับซื้อ: {current_price['buy_price']} บาท {buy_status} {diff_buy} บาท\n"
        )

        # Send notifications
        send_line_notification(message_hsh, line_personal_token)
        send_line_notification(message_hsh, line_group_token)

    else:
        print("No price change detected. Notification skipped.")

with DAG(
    'nofify_gold_price_pipeline',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Pipeline สำหรับแจ้งเตือนเมื่อราคาทองคำมีการเปลี่ยนแปลง',
    schedule_interval='@hourly',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['gold_price']
) as dag:

    notify_hsh_price_change_task = PythonOperator(
        task_id='notify_hsh_price_change',
        python_callable=notify_hsh_price_change,
        provide_context=True
    )

    notify_ref_price_change_task = PythonOperator(
        task_id='notify_ref_price_change',
        python_callable=notify_ref_price_change,
        provide_context=True
    )

    notify_hsh_price_change_task >> notify_ref_price_change_task
