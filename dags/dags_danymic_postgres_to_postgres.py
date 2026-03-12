from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


SOURCE_POSTGRES_CONN_ID = "postgres_conn"
TARGET_POSTGRES_CONN_ID = "postgres_conn"

CHUNK_SIZE = 5000


with DAG(
    dag_id="dynamic_postgres_to_postgres_etl_meta",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def get_table_configs():
        source_hook = PostgresHook(postgres_conn_id=SOURCE_POSTGRES_CONN_ID)

        sql = """
        SELECT
            source_table,
            target_table,
            pk_column,
            columns,
            where_clause
        FROM etl_meta
        WHERE 1=1
          AND enable_yn = 'Y'
          AND job_type = 'DAILY'
        """

        rows = source_hook.get_records(sql)

        configs = []

        for r in rows:
            configs.append(
                {
                    "source_table": r[0],
                    "target_table": r[1],
                    "pk_columns": [c.strip() for c in r[2].split(",")],   # 복합 PK 지원
                    "columns": [c.strip() for c in r[3].split(",")],
                    "where_clause": r[4],
                }
            )

        return configs

    @task
    def run_etl(table_config: dict):
        source_table = table_config["source_table"]
        target_table = table_config["target_table"]
        pk_columns = table_config["pk_columns"]
        columns = table_config["columns"]
        where_clause = table_config["where_clause"]

        stg_table = f"stg_{target_table}"

        column_list_sql = ", ".join(columns)

        source_sql = f"""
            SELECT {column_list_sql}
            FROM {source_table}
            WHERE {where_clause}
        """

        # MERGE ON 절
        merge_on_sql = " AND ".join(
            [f"t.{pk} = s.{pk}" for pk in pk_columns]
        )

        # UPDATE 대상은 PK 제외 컬럼만
        non_pk_columns = [c for c in columns if c not in pk_columns]

        update_set_sql = ", ".join(
            [f"{col} = s.{col}" for col in non_pk_columns]
        )

        insert_columns_sql = ", ".join(columns)
        insert_values_sql = ", ".join([f"s.{col}" for col in columns])

        # PostgreSQL 15+ 에서 MERGE 사용 가능
        if non_pk_columns:
            merge_sql = f"""
                MERGE INTO {target_table} AS t
                USING {stg_table} AS s
                ON ({merge_on_sql})
                WHEN MATCHED THEN
                    UPDATE SET {update_set_sql}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns_sql})
                    VALUES ({insert_values_sql})
            """
        else:
            # 모든 컬럼이 PK인 극단적 케이스 대응
            merge_sql = f"""
                MERGE INTO {target_table} AS t
                USING {stg_table} AS s
                ON ({merge_on_sql})
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns_sql})
                    VALUES ({insert_values_sql})
            """

        create_stg_sql = f"""
            CREATE TABLE IF NOT EXISTS {stg_table}
            (LIKE {target_table} INCLUDING ALL)
        """

        truncate_stg_sql = f"TRUNCATE TABLE {stg_table}"

        source_hook = PostgresHook(postgres_conn_id=SOURCE_POSTGRES_CONN_ID)
        target_hook = PostgresHook(postgres_conn_id=TARGET_POSTGRES_CONN_ID)

        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor(name=f"csr_{target_table}")  # 서버사이드 커서
        source_cursor.itersize = CHUNK_SIZE

        total_rows = 0

        try:
            # 1) STG 테이블 생성
            target_hook.run(create_stg_sql)

            # 2) STG 테이블 비우기
            target_hook.run(truncate_stg_sql)

            # 3) PostgreSQL SELECT 실행
            source_cursor.execute(source_sql)

            # 4) STG 테이블에 chunk 단위 INSERT
            while True:
                rows = source_cursor.fetchmany(size=CHUNK_SIZE)

                if not rows:
                    break

                target_hook.insert_rows(
                    table=stg_table,
                    rows=rows,
                    target_fields=columns,
                    commit_every=CHUNK_SIZE,
                    executemany=True,
                )

                total_rows += len(rows)

                print(
                    f"{source_table} -> {stg_table} "
                    f"chunk={len(rows)} total={total_rows}"
                )

            # 5) STG -> TARGET MERGE
            if total_rows > 0:
                target_hook.run(merge_sql)
                print(
                    f"{stg_table} -> {target_table} MERGE completed, total={total_rows}"
                )
            else:
                print(f"{source_table}: no rows fetched, MERGE skipped")

        finally:
            source_cursor.close()
            source_conn.close()

    table_configs = get_table_configs()
    run_etl.expand(table_config=table_configs)