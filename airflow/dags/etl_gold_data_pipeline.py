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
        with open(raw_csv_path, "w", newline="") as raw_csv:
            writer = csv.DictWriter(raw_csv, fieldnames=["GoldType", "Buy", "Sell", "Timestamp"])
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
        local_csv_directory = os.path.join(os.getenv('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'files', 'cleand_csv')
        os.makedirs(local_csv_directory, exist_ok=True)
        hsh_csv_path = os.path.join(local_csv_directory, "gold_hsh_prices.csv")
        ref_csv_path = os.path.join(local_csv_directory, "gold_ref_prices.csv")

        with open(raw_csv_path, "r") as raw_csv:
            reader = csv.DictReader(raw_csv)
            hsh_data = [row for row in reader if row.get("GoldType") == "HSH"]
            raw_csv.seek(0)
            ref_data = [row for row in reader if row.get("GoldType") == "REF"]

        fieldnames = ["GoldType", "Buy", "Sell", "Timestamp", "Day", "Month", "Year", "Hour", "Minute", "Second"]

        # Save HSH data
        with open(hsh_csv_path, "w", newline="") as hsh_csv:
            writer = csv.DictWriter(hsh_csv, fieldnames=fieldnames)
            writer.writeheader()
            for row in hsh_data:
                writer.writerow({
                    "GoldType": row.get("GoldType"),
                    "Buy": float(row.get("Buy", "0").replace(",", "")),
                    "Sell": float(row.get("Sell", "0").replace(",", "")),
                    "Timestamp": row.get("Timestamp"),
                    "Day": DAY,
                    "Month": MONTH,
                    "Year": YEAR,
                    "Hour": HOUR,
                    "Minute": MINUTE,
                    "Second": SECOND
                })

        # Save REF data
        with open(ref_csv_path, "w", newline="") as ref_csv:
            writer = csv.DictWriter(ref_csv, fieldnames=fieldnames)
            writer.writeheader()
            for row in ref_data:
                writer.writerow({
                    "GoldType": row.get("GoldType"),
                    "Buy": float(row.get("Buy", "0").replace(",", "")),
                    "Sell": float(row.get("Sell", "0").replace(",", "")),
                    "Timestamp": row.get("Timestamp"),
                    "Day": DAY,
                    "Month": MONTH,
                    "Year": YEAR,
                    "Hour": HOUR,
                    "Minute": MINUTE,
                    "Second": SECOND
                })

        kwargs['ti'].xcom_push(key='hsh_csv_path', value=hsh_csv_path)
        kwargs['ti'].xcom_push(key='ref_csv_path', value=ref_csv_path)

        print(f"Transformed data saved to {hsh_csv_path} and {ref_csv_path}")

    except Exception as e:
        print(f"Error transforming CSV: {e}")

def ensure_table_exists(cursor, table_name):
    """Check if a table exists in the database and create it if not."""
    create_table_queries = {
        "hsh_prices": """
            CREATE TABLE IF NOT EXISTS hsh_prices (
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
            print(f"Loading data from {csv_path} into table {table_name}")
            with open(csv_path, "r") as file:
                cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", file)

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error loading data to Postgres: {e}")


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
