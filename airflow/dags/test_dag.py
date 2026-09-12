from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def start_pipeline():
    logging.info("E-Commerce pipeline started")
    print("E-Commerce pipeline started")


def end_pipeline():
    logging.info("E-Commerce pipeline test completed")
    print("E-Commerce pipeline test completed")


with DAG(
    dag_id="test_dag",
    default_args=default_args,
    description="Initial test DAG for E-Commerce Data Engineering pipeline",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test", "ecommerce"],
) as dag:

    start_task = PythonOperator(
        task_id="start_task",
        python_callable=start_pipeline,
    )

    end_task = PythonOperator(
        task_id="end_task",
        python_callable=end_pipeline,
    )

    start_task >> end_task
