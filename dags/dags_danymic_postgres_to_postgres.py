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
            where_clause,
            load_option
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
                    "pk_columns": [c.strip() for c in r[2].split(",")],
                    "columns": [c.strip() for c in r[3].split(",")],
                    "where_clause": r[4],
                    "load_option": r[5].strip().lower() if r[5] else "di",
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
        load_option = table_config["load_option"]

        stg_table = f"stg_{target_table}"

        column_list_sql = ", ".join(columns)

        source_sql = f"""
            SELECT {column_list_sql}
            FROM {source_table}
            WHERE {where_clause}
        """

        create_stg_sql = f"""
            CREATE TABLE IF NOT EXISTS {stg_table}
            (LIKE {target_table} INCLUDING ALL)
        """

        truncate_stg_sql = f"TRUNCATE TABLE {stg_table}"
        truncate_target_sql = f"TRUNCATE TABLE {target_table}"

        insert_columns_sql = ", ".join(columns)
        select_columns_sql = ", ".join([f"s.{col}" for col in columns])

        insert_sql = f"""
            INSERT INTO {target_table} ({insert_columns_sql})
            SELECT {select_columns_sql}
            FROM {stg_table} s
        """

        # PK 조인 조건
        pk_join_condition_sql = " AND ".join(
            [f"t.{pk} = s.{pk}" for pk in pk_columns]
        )

        # PK 제외 컬럼
        non_pk_columns = [c for c in columns if c not in pk_columns]

        # ui: UPDATE SQL
        if non_pk_columns:
            update_set_sql = ", ".join(
                [f"{col} = s.{col}" for col in non_pk_columns]
            )

            update_sql = f"""
                UPDATE {target_table} t
                SET {update_set_sql}
                FROM {stg_table} s
                WHERE {pk_join_condition_sql}
            """
        else:
            update_sql = None

        # ui/di 공통으로 쓸 INSERT NOT EXISTS SQL
        not_exists_condition_sql = " AND ".join(
            [f"t.{pk} = s.{pk}" for pk in pk_columns]
        )

        insert_not_exists_sql = f"""
            INSERT INTO {target_table} ({insert_columns_sql})
            SELECT {select_columns_sql}
            FROM {stg_table} s
            WHERE NOT EXISTS (
                SELECT 1
                FROM {target_table} t
                WHERE {not_exists_condition_sql}
            )
        """

        # di: DELETE SQL
        delete_sql = f"""
            DELETE FROM {target_table} t
            WHERE EXISTS (
                SELECT 1
                FROM {stg_table} s
                WHERE {pk_join_condition_sql}
            )
        """

        source_hook = PostgresHook(postgres_conn_id=SOURCE_POSTGRES_CONN_ID)
        target_hook = PostgresHook(postgres_conn_id=TARGET_POSTGRES_CONN_ID)

        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor(name=f"csr_{target_table}")
        source_cursor.itersize = CHUNK_SIZE

        total_rows = 0

        try:
            # 1) STG 테이블 생성
            target_hook.run(create_stg_sql)

            # 2) STG 테이블 비우기
            target_hook.run(truncate_stg_sql)

            # 3) 소스 SELECT 실행
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

            # 5) load_option 에 따라 TARGET 적재
            if total_rows > 0:
                if load_option == "ui":
                    if update_sql:
                        target_hook.run(update_sql)
                        print(f"{target_table} UPDATE completed")

                    target_hook.run(insert_not_exists_sql)
                    print(
                        f"{stg_table} -> {target_table} INSERT completed (UI), total={total_rows}"
                    )

                elif load_option == "di":
                    target_hook.run(delete_sql)
                    print(f"{target_table} DELETE completed")

                    target_hook.run(insert_sql)
                    print(
                        f"{stg_table} -> {target_table} INSERT completed (DI), total={total_rows}"
                    )

                elif load_option == "ti":
                    target_hook.run(truncate_target_sql)
                    print(f"{target_table} TRUNCATE completed")

                    target_hook.run(insert_sql)
                    print(
                        f"{stg_table} -> {target_table} INSERT completed (TI), total={total_rows}"
                    )

                else:
                    raise ValueError(
                        f"Invalid load_option: {load_option}. "
                        f"Allowed values are ui, di, ti."
                    )

            else:
                print(f"{source_table}: no rows fetched, target load skipped")

        finally:
            source_cursor.close()
            source_conn.close()

    table_configs = get_table_configs()
    run_etl.expand(table_config=table_configs)