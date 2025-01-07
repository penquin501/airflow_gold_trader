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

    hsh_query = """
        SELECT 
            DATE(TO_TIMESTAMP(timestamp, 'YYYYMMDDHH24MISS')) as date, 
            AVG(sell) as avg_sell
        FROM hsh_prices
        GROUP BY DATE(TO_TIMESTAMP(timestamp, 'YYYYMMDDHH24MISS'))
        ORDER BY DATE(TO_TIMESTAMP(timestamp, 'YYYYMMDDHH24MISS'));
    """

    ref_query = """
        SELECT 
            DATE(TO_TIMESTAMP(timestamp, 'YYYYMMDDHH24MISS')) as date, 
            AVG(sell) as avg_sell
        FROM ref_prices
        GROUP BY DATE(TO_TIMESTAMP(timestamp, 'YYYYMMDDHH24MISS'))
        ORDER BY DATE(TO_TIMESTAMP(timestamp, 'YYYYMMDDHH24MISS'));
    """


    hsh_data = pd.read_sql_query(hsh_query, conn)
    ref_data = pd.read_sql_query(ref_query, conn)

    conn.close()

    # Push data to XCom
    kwargs['ti'].xcom_push(key='hsh_data', value=hsh_data.to_json())
    kwargs['ti'].xcom_push(key='ref_data', value=ref_data.to_json())

# Transform Task
def calculate_datamart(**kwargs):
    hsh_data = pd.read_json(kwargs['ti'].xcom_pull(key='hsh_data'))
    ref_data = pd.read_json(kwargs['ti'].xcom_pull(key='ref_data'))
    
    hsh_metrics = calculate_metrics(hsh_data)
    ref_metrics = calculate_metrics(ref_data)

    datamart = {
        'hsh_metrics': hsh_metrics,
        'ref_metrics': ref_metrics
    }

    kwargs['ti'].xcom_push(key='datamart', value=datamart)

def calculate_metrics(data):
    try:
        if 'avg_sell' not in data.columns:
            raise KeyError("Column 'avg_sell' is missing from the data")

        if 'date' not in data.columns:
            raise KeyError("Column 'date' is missing from the data")

        grouped = data.groupby('date')
        metrics_list = []

        for date, group in grouped:
            mean_price = group['avg_sell'].mean()
            median_price = group['avg_sell'].median()

            if len(group) > 1:
                group['price_change'] = group['avg_sell'].pct_change() * 100
                price_change_avg = group['price_change'].mean(skipna=True)
            else:
                price_change_avg = 0

            if len(group) >= 5:
                group['volatility'] = group['avg_sell'].rolling(window=5).std()
                price_volatility_avg = group['volatility'].mean(skipna=True)
            else:
                price_volatility_avg = 0

            metrics_list.append({
                'date': str(date),
                'mean_price': float(mean_price),
                'median_price': float(median_price),
                'price_change_avg': float(price_change_avg),
                'price_volatility_avg': float(price_volatility_avg)
            })

        return metrics_list
    except Exception as e:
        print(f"Error calculating metrics: {e}")
        return []

# Load Task
def load_datamart_to_postgres(**kwargs):
    datamart = kwargs['ti'].xcom_pull(key='datamart')

    conn = get_postgres_connection("gold_postgres")
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS datamart (
            id SERIAL PRIMARY KEY,
            table_name TEXT,
            date DATE NOT NULL,
            mean_price FLOAT,
            median_price FLOAT,
            price_change_avg FLOAT,
            price_volatility_avg FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(table_name, date)
        );
    """)

    # Insert or update data
    for table_name, metrics_list in datamart.items():
        for metrics in metrics_list:
            cursor.execute(
                """
                INSERT INTO datamart (table_name, date, mean_price, median_price, price_change_avg, price_volatility_avg)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (table_name, date) DO UPDATE
                SET mean_price = EXCLUDED.mean_price,
                    median_price = EXCLUDED.median_price,
                    price_change_avg = EXCLUDED.price_change_avg,
                    price_volatility_avg = EXCLUDED.price_volatility_avg;
                """,
                (
                    table_name,
                    metrics['date'],
                    float(metrics.get('mean_price', 0)),
                    float(metrics.get('median_price', 0)),
                    float(metrics.get('price_change_avg', 0)),
                    float(metrics.get('price_volatility_avg', 0))
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

    transform_task = PythonOperator(
        task_id='calculate_datamart',
        python_callable=calculate_datamart,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_datamart_to_postgres',
        python_callable=load_datamart_to_postgres,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
