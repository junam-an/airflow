from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from common.static_postgres_to_file_etl_daily import (
    run_postgres_to_file_etl,
)


DAG_ID = "static_postgres_to_file_etl_daily_01"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "postgres", "static"],
) as dag:

    @task(task_id="TASK1")
    def TASK1(**context):
        run_postgres_to_file_etl(
            dag_id="DYNAMIC_POSTGRES_TO_FILE_ETL_META_2",
            task_name="TASK1",
            **context,
        )

    @task(task_id="TASK3")
    def TASK2(**context):
        run_postgres_to_file_etl(
            dag_id="DYNAMIC_POSTGRES_TO_FILE_ETL_META_2",
            task_name="TASK3",
            **context,
        )


    v_TASK1 = TASK1()
    v_TASK2 = TASK2()

    v_TASK1 >> v_TASK2