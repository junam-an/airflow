from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from common.static_odbc_file_to_oracle_etl_minute import run_file_to_odbc_etl


DAG_ID = "STATIC_FILE_TO_ORACLE_ETL_META_3"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "postgres", "static"],
) as dag:

    @task(task_id="TASK1")
    def TASK1(**context):
        run_file_to_odbc_etl(
            dag_id="DYNAMIC_ODBC_FILE_TO_ORACLE_ETL_META_3",
            task_name="TASK1",
            **context,
        )

    @task(task_id="TASK2")
    def TASK2(**context):
        run_file_to_odbc_etl(
            dag_id="DYNAMIC_ODBC_FILE_TO_ORACLE_ETL_META_3",
            task_name="TASK2",
            **context,
        )


    v_TASK1 = TASK1()
    v_TASK2 = TASK2()

    v_TASK1 >> v_TASK2