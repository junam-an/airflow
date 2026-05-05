from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from common.static_odbc_oracle_to_file_etl_minute import run_odbc_to_file_common


DAG_ID = "STATIC_ORACLE_TO_FILE_ETL_META_3"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "postgres", "static"],
) as dag:

    @task(task_id="TASK1")
    def TASK1(**context):
        run_odbc_to_file_common(
            dag_id="DYNAMIC_ODBC_ORACLE_TO_FILE_ETL_META_3",
            task_name="TASK1",
            **context,
        )

    @task(task_id="TASK2")
    def TASK2(**context):
        run_odbc_to_file_common(
            dag_id="DYNAMIC_ODBC_ORACLE_TO_FILE_ETL_META_3",
            task_name="TASK2",
            **context,
        )


    v_TASK1 = TASK1()
    v_TASK2 = TASK2()

    v_TASK1 >> v_TASK2