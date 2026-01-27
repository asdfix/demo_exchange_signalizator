import logging

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from pybit.unified_trading import HTTP
import pandas as pd
import s3fs

# Настройки
S3_BUCKET = "s3://raw-data/quotes"
MINIO_ENDPOINT = "http://localhost:9000"  # Замените на ваш адрес
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")
SYMBOL = "BTCUSDT"


def fetch_and_save_bybit_data(execution_date, **kwargs):
    logging.info(f"fetch_and_save_bybit_data для {execution_date}")

    # 1. Рассчитываем временной интервал (сутки)
    # execution_date приходит из Airflow как начало периода
    start_time = int(execution_date.timestamp() * 1000)
    end_time = int((execution_date + timedelta(days=1)).timestamp() * 1000)

    session = HTTP(testnet=False)

    all_candles = []
    current_start = start_time

    # Bybit отдает максимум 1000 свечей за раз, для суток (1440 мин) нужно 2 итерации
    while current_start < end_time:
        response = session.get_kline(
            category="linear",
            symbol=SYMBOL,
            interval="1",
            start=current_start,
            end=end_time,
            limit=1000
        )

        data = response.get('result', {}).get('list', [])
        if not data:
            break

        all_candles.extend(data)
        # Следующий запрос начинаем с времени последней полученной свечи + 1 минута
        current_start = int(data[0][0]) + 60000

    if not all_candles:
        print("Данные не найдены")
        return

    # 2. Обработка в Pandas
    columns = ["timestamp", "open", "high", "low", "close", "volume", "turnover"]
    df = pd.DataFrame(all_candles, columns=columns)

    # Конвертируем типы
    df["timestamp"] = pd.to_datetime(df["timestamp"].astype(float), unit='ms')
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    # Добавляем колонки для партиционирования
    df["year"] = execution_date.year
    df["month"] = execution_date.month
    df["day"] = execution_date.day

    # 3. Сохранение в MinIO
    storage_options = {
        "key": MINIO_ACCESS_KEY,
        "secret": MINIO_SECRET_KEY,
        "client_kwargs": {"endpoint_url": MINIO_ENDPOINT}
    }

    # Путь включает биржу и символ, остальное сделает partition_cols
    full_path = f"{S3_BUCKET}/exchange=bybit/symbol={SYMBOL}/interval=1m/"

    df.to_parquet(
        full_path,
        engine='pyarrow',
        partition_cols=['year', 'month', 'day'],
        storage_options=storage_options,
        index=False
    )
    logging.info(f"Записано {len(df)} строк в {full_path}")


# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': True,  # Важно для последовательного сбора истории
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'raw_historical_bybit_to_s3',
        default_args=default_args,
        description='Забирает минутные свечи с Bybit и кладет в MinIO',
        schedule='@daily',  # Запуск раз в сутки
        start_date=datetime(2021, 1, 1),  # Откуда начать сбор истории
        catchup=True,  # Позволяет Airflow "нагнать" историю с start_date
        max_active_runs=3,  # Ограничение, чтобы не спамить API Bybit
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    fetch_task = PythonOperator(
        task_id='fetch_bybit_candles',
        python_callable=fetch_and_save_bybit_data
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> fetch_task >> end