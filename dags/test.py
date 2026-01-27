from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys
import subprocess
import logging


def log_python_env():
    logger = logging.getLogger("airflow.task")

    # Путь к интерпретатору
    logger.info("Python executable: %s", sys.executable)

    # Версия Python
    logger.info("Python version: %s", sys.version)

    # Список установленных пакетов
    result = subprocess.run(
        [sys.executable, "-m", "pip", "freeze"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )

    if result.stdout:
        logger.info("Installed packages:\n%s", result.stdout)

    if result.stderr:
        logger.warning("pip stderr:\n%s", result.stderr)


with DAG(
    dag_id="log_python_environment",
    start_date=datetime(2026, 1, 1),
    max_active_tasks=None,
    catchup=False,
    tags=["debug", "env"],
) as dag:

    log_env = PythonOperator(
        task_id="log_python_environment",
        python_callable=log_python_env,
    )

    log_env
