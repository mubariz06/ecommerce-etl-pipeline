"""
ecommerce_etl_dag.py
----------------------
Airflow DAG orchestrating the end-to-end Extract -> Transform -> Load
-> Data Quality pipeline for e-commerce order data.

Schedule: daily
Owner:    Mohammed Mubariz
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.append("/opt/airflow/scripts")

from extract import extract_orders          # noqa: E402
from transform import transform_orders      # noqa: E402
from load import load_orders                # noqa: E402
from data_quality import run_quality_checks # noqa: E402

default_args = {
    "owner": "mohammed_mubariz",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

with DAG(
    dag_id="ecommerce_etl_pipeline",
    description="End-to-end ETL pipeline: extract -> PySpark transform -> load -> data quality",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "pyspark", "portfolio"],
) as dag:

    def _extract(**context):
        ds = context["ds"]
        path = extract_orders(execution_date=ds)
        context["ti"].xcom_push(key="extracted_path", value=path)

    def _transform(**context):
        ds = context["ds"]
        extracted_path = context["ti"].xcom_pull(key="extracted_path", task_ids="extract_orders")
        processed_path = transform_orders(extracted_path, ds)
        context["ti"].xcom_push(key="processed_path", value=processed_path)

    def _load(**context):
        processed_path = context["ti"].xcom_pull(key="processed_path", task_ids="transform_orders")
        load_orders(processed_path)

    def _quality_check(**context):
        processed_path = context["ti"].xcom_pull(key="processed_path", task_ids="transform_orders")
        run_quality_checks(processed_path)

    extract_task = PythonOperator(
        task_id="extract_orders",
        python_callable=_extract,
    )

    transform_task = PythonOperator(
        task_id="transform_orders",
        python_callable=_transform,
    )

    quality_check_task = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=_quality_check,
    )

    load_task = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=_load,
    )

    # Quality gate runs BEFORE load so bad data never reaches the warehouse
    extract_task >> transform_task >> quality_check_task >> load_task
