import asyncio
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from eth_fraud_detection.apps.feature_extraction.tasks.feature_extraction import extract_features


def run_feature_pipeline():
    asyncio.run(extract_features())


with DAG(
    dag_id="neo4j_to_postgres_features",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["neo4j", "postgres", "features"],
) as dag:

    PythonOperator(
        task_id="extract_load",
        python_callable=run_feature_pipeline,
        retries=3,
        retry_delay=timedelta(minutes=5),
    )