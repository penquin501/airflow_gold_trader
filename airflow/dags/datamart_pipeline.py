import psycopg2
import pandas as pd
from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Utility function to get a database connection
def get_postgres_connection(conn_id):
    connection = BaseHook.get_connection(conn_id)
    conn = psycopg2.connect(
        dbname=connection.schema,
        user=connection.login,
        password=connection.password,
        host=connection.host,
        port=connection.port
    )
    return conn

# Extract Task
def extract_data(**kwargs):
    conn = get_postgres_connection("postgres_default")

    # Query for hsh_prices
    hsh_query = """
        WITH hsh_data AS (
            SELECT 
                DATE(TO_TIMESTAMP(timestamp, 'YYYYMMDDHH24MISS')) AS date,
                sell
            FROM hsh_prices
        ),
        hsh_grouped AS (
            SELECT 
                date,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sell) AS median_price,
                AVG(sell) AS avg_sell,
                MIN(sell) AS min_price,
                MAX(sell) AS max_price,
                STDDEV_SAMP(sell) AS volatility,
                100 * (AVG(sell) - LAG(AVG(sell)) OVER (ORDER BY date)) / NULLIF(LAG(AVG(sell)) OVER (ORDER BY date), 0) AS price_change
            FROM hsh_data
            GROUP BY date
        )
        SELECT 
            date,
            median_price,
            avg_sell,
            min_price,
            max_price,
            volatility,
            COALESCE(price_change, 0) AS price_change
        FROM hsh_grouped
        ORDER BY date;
    """

    # Query for ref_prices
    ref_query = """
        WITH ref_data AS (
            SELECT 
                DATE(TO_TIMESTAMP(timestamp, 'YYYYMMDDHH24MISS')) AS date,
                sell
            FROM ref_prices
        ),
        ref_grouped AS (
            SELECT 
                date,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sell) AS median_price,
                AVG(sell) AS avg_sell,
                MIN(sell) AS min_price,
                MAX(sell) AS max_price,
                STDDEV_SAMP(sell) AS volatility,
                100 * (AVG(sell) - LAG(AVG(sell)) OVER (ORDER BY date)) / NULLIF(LAG(AVG(sell)) OVER (ORDER BY date), 0) AS price_change
            FROM ref_data
            GROUP BY date
        )
        SELECT 
            date,
            median_price,
            avg_sell,
            min_price,
            max_price,
            volatility,
            COALESCE(price_change, 0) AS price_change
        FROM ref_grouped
        ORDER BY date;
    """

    hsh_data = pd.read_sql_query(hsh_query, conn)
    ref_data = pd.read_sql_query(ref_query, conn)
    
    for column in ['median_price', 'avg_sell', 'volatility', 'price_change']:
        if column not in hsh_data.columns:
            print(f"Warning: '{column}' column is missing. Adding default values.")
            hsh_data[column] = None
    conn.close()

    # Push data to XCom
    kwargs['ti'].xcom_push(key='hsh_data', value=hsh_data.to_json())
    kwargs['ti'].xcom_push(key='ref_data', value=ref_data.to_json())

# Load Task
def load_datamart_to_postgres(**kwargs):
    hsh_data = pd.read_json(kwargs['ti'].xcom_pull(key='hsh_data'))
    ref_data = pd.read_json(kwargs['ti'].xcom_pull(key='ref_data'))

    conn = get_postgres_connection("gold_postgres")
    cursor = conn.cursor()

    # Create tables if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS datamart_hsh (
            id SERIAL PRIMARY KEY,
            date DATE UNIQUE,
            median_price FLOAT,
            avg_sell FLOAT,
            min_price FLOAT,
            max_price FLOAT,
            price_change FLOAT,
            volatility FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS datamart_ref (
            id SERIAL PRIMARY KEY,
            date DATE UNIQUE,
            median_price FLOAT,
            avg_sell FLOAT,
            min_price FLOAT,
            max_price FLOAT,
            price_change FLOAT,
            volatility FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # เพิ่ม Index บนคอลัมน์ `date`
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_datamart_hsh_date ON datamart_hsh(date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_datamart_ref_date ON datamart_ref(date);")

    # Insert data into datamart_hsh
    for _, row in hsh_data.iterrows():
        cursor.execute(
            """
            INSERT INTO datamart_hsh (date, median_price, avg_sell, min_price, max_price, price_change, volatility)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
                median_price = EXCLUDED.median_price,
                avg_sell = EXCLUDED.avg_sell,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                price_change = EXCLUDED.price_change,
                volatility = EXCLUDED.volatility;
            """,
            (
                row['date'],
                row['median_price'],
                row['avg_sell'],
                row['min_price'],
                row['max_price'],
                row['price_change'],
                row['volatility']
            )
        )

    # Insert data into datamart_ref
    for _, row in ref_data.iterrows():
        cursor.execute(
            """
            INSERT INTO datamart_ref (date, median_price, avg_sell, min_price, max_price, price_change, volatility)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
                median_price = EXCLUDED.median_price,
                avg_sell = EXCLUDED.avg_sell,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                price_change = EXCLUDED.price_change,
                volatility = EXCLUDED.volatility;
            """,
            (
                row['date'],
                row['median_price'],
                row['avg_sell'],
                row['min_price'],
                row['max_price'],
                row['price_change'],
                row['volatility']
            )
        )

    conn.commit()
    cursor.close()
    conn.close()

# Airflow DAG
default_args = {
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'datamart_pipeline',
    default_args=default_args,
    description='Pipeline for generating datamart from gold price data',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['datamart', 'gold_price']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_datamart_to_postgres',
        python_callable=load_datamart_to_postgres,
        provide_context=True
    )

    extract_task >> load_task
