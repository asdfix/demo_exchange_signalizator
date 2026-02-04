import logging

import duckdb
from airflow.models import Variable
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import requests
import pandas as pd
import io

# Константы

LATITUDE = 41.44
LONGITUDE = -8.30

BUCKET_NAME = 'weather'  # Не забудьте создать этот бакет в UI MinIO
MINIO_CONN_ID = 'minio_conn'

# Используемые таблицы в DAG
LAYER = "weather"
SOURCE = "archive"

# S3
ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
SECRET_KEY = Variable.get("MINIO_SECRET_KEY")


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


@dag(
    dag_id='weather_to_minio_partitioned',
    start_date=datetime(2026, 1, 1),
    schedule='@daily',
    # description=SHORT_DESCRIPTION,
    catchup=True,
    tags=['minio', 'weather']
)
def weather_minio_dag():
    @task
    def extract_weather(ds=None, **context):
        # Используем архивный API для исторических данных
        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={LATITUDE}&longitude={LONGITUDE}&start_date={ds}&end_date={ds}&hourly=temperature_2m&timezone=auto"
        response = requests.get(url)
        return response.json()

    @task
    def load_to_minio(data, ds=None, **context):
        # 1. Трансформация в Pandas и Parquet
        df = pd.DataFrame(data['hourly'])
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        # 2. Формирование пути (Hive-partitioning)
        dt = datetime.strptime(ds, '%Y-%m-%d')
        s3_key = f"year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/data.parquet"

        # # 3. Загрузка через S3Hook с указанием MinIO endpoint
        # # Берем endpoint_url из настроек Connection
        # s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        #
        # s3_hook.load_bytes(
        #     bytes_data=buffer.getvalue(),
        #     key=s3_key,
        #     bucket_name=BUCKET_NAME,
        #     replace=True
        # )
        # return s3_key

        start_date, end_date = get_dates(**context)
        logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
        con = duckdb.connect()

        con.sql(
            f"""
                SET TIMEZONE='UTC';
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_url_style = 'path';
                SET s3_endpoint = 'minio:9000';
                SET s3_access_key_id = '{ACCESS_KEY}';
                SET s3_secret_access_key = '{SECRET_KEY}';
                SET s3_use_ssl = FALSE;

                COPY
                (
                    SELECT
                        *
                    FROM
                        read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
                ) TO 's3://{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';

                """,
        )

        con.close()
        logging.info(f"✅ Download for date success: {start_date}")

    weather_data = extract_weather()
    load_to_minio(weather_data)


weather_minio_dag()