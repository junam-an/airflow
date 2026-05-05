from __future__ import annotations

from datetime import datetime

from airflow import DAG
#from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.bash import BashOperator

from common.postgres_to_postgres_etl_daily import create_postgres_to_postgres_tasks


DAG_ID = "DYNAMIC_FILE_TO_POSTGRES_ETL_META_2"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    tags=["etl", "postgres", "sales"],
) as dag:

    run_script_1 = BashOperator(
        task_id="run_script_1",
        #bash_command="sh /opt/airflow/scripts/script1.sh",
        bash_command="echo test",
    )

    table_configs, run_etl_tasks = create_postgres_to_postgres_tasks(
        dag_id=DAG_ID
    )

    run_script_2 = BashOperator(
        task_id="run_script_2",
        #bash_command="sh /opt/airflow/scripts/script2.sh",
        bash_command="echo test",
    )

    run_script_1 >> table_configs >> run_etl_tasks >> run_script_2