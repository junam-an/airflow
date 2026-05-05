from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from common.static_postgres_to_postgres_etl_daily import (
    run_postgres_to_postgres_etl,
)


DAG_ID = "STATIC_POSTGRES_TO_POSTGRES_ETL_META_3"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "postgres", "static"],
) as dag:

    @task(task_id="TASK1")
    def TASK1(**context):
        return run_postgres_to_postgres_etl(
            dag_id="DYNAMIC_SALES_POSTGRES_TO_POSTGRES_ETL",
            task_name="TASK1",
            **context,
        )

    @task(task_id="TASK2")
    def TASK2(**context):
        return run_postgres_to_postgres_etl(
            dag_id="DYNAMIC_SALES_POSTGRES_TO_POSTGRES_ETL",
            task_name="TASK2",
            **context,
        )


    v_TASK1 = TASK1()
    v_TASK2 = TASK2()

    v_TASK1 >> v_TASK2