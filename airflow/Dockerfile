FROM apache/airflow:2.8.3
USER airflow
WORKDIR /app
COPY . /app

COPY requirements.txt /requirements.txt

COPY .env /opt/airflow
RUN pip install --no-cache-dir flask psycopg2
RUN pip install --no-cache-dir selenium
RUN pip install --no-cache-dir --user -r /requirements.txt
CMD ["gunicorn", "-k", "uvicorn.workers.UvicornWorker", "gold_price_api:app", "--bind", "0.0.0.0:80"]
