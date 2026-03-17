from __future__ import annotations

from airflow import DAG
from airflow.decorators import task
from airflow.providers.odbc.hooks.odbc import OdbcHook

from datetime import datetime
import os
import csv


CONN_ID = "oracle23c_odbc_conn"
OUTPUT_DIR = "/opt/airflow/source"
OUTPUT_FILE = "ora_test.csv"


with DAG(
    dag_id="oracle_odbc_to_csv",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def extract_and_save():

        # 1. ODBC 연결
        hook = OdbcHook(odbc_conn_id=CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # 2. 쿼리 실행
        sql = "select 'A' col1, 'B' col2, 'C' col3 from dual"
        cursor.execute(sql)

        # 3. 결과 fetch
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        # 4. 디렉토리 생성 (없으면)
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        file_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

        # 5. CSV 파일 저장
        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)

            # header
            writer.writerow(columns)

            # data
            writer.writerows(rows)

        cursor.close()
        conn.close()

        print(f"File created: {file_path}")

    extract_and_save()