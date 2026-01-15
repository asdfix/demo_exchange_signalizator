FROM apache/airflow:3.1.5-python3.12

USER airflow

COPY requirements.txt /opt/airflow/

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /opt/airflow/requirements.txt
