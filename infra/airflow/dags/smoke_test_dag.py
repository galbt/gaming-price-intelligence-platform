from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


def check_iceberg_catalog_reachable():
    response = requests.get("http://iceberg-rest:8181/v1/config", timeout=5)
    response.raise_for_status()


with DAG(
    dag_id="smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(
        task_id="check_iceberg_catalog_reachable",
        python_callable=check_iceberg_catalog_reachable,
    )
