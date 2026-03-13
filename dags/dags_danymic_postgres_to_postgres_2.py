from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


SOURCE_POSTGRES_CONN_ID = "postgres_conn"
TARGET_POSTGRES_CONN_ID = "postgres_conn"

CHUNK_SIZE = 5000


def parse_column_mapping(raw_mapping: str | None) -> dict[str, str]:
    """
    source alias -> target column 매핑 파싱
    지원 형식:
      1) JSON: {"src_a":"a","src_b":"b"}
      2) 문자열: "src_a:a, src_b:b"
      3) 문자열: "src_a=a, src_b=b"
    """
    if not raw_mapping or not raw_mapping.strip():
        return {}

    raw_mapping = raw_mapping.strip()

    # 1) JSON 형식 우선 처리
    try:
        parsed = json.loads(raw_mapping)

        if isinstance(parsed, dict):
            return {str(k).strip(): str(v).strip() for k, v in parsed.items()}

        if isinstance(parsed, list):
            result = {}
            for item in parsed:
                if isinstance(item, dict) and "source" in item and "target" in item:
                    result[str(item["source"]).strip()] = str(item["target"]).strip()
            return result
    except Exception:
        pass

    # 2) src:tar, src2:tar2 또는 src=tar 형식 처리
    result = {}

    for pair in raw_mapping.split(","):
        pair = pair.strip()
        if not pair:
            continue

        if ":" in pair:
            src, tgt = pair.split(":", 1)
        elif "=" in pair:
            src, tgt = pair.split("=", 1)
        else:
            raise ValueError(
                f"Invalid column_mapping format: {raw_mapping}"
            )

        src = src.strip()
        tgt = tgt.strip()

        if not src or not tgt:
            raise ValueError(
                f"Invalid column_mapping pair: {pair}"
            )

        result[src] = tgt

    return result


with DAG(
    dag_id="dynamic_postgres_to_postgres_etl_meta_2",
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
            source_exec_sql,
            column_mapping,
            load_option,
            stg_drop_yn
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
                    "pk_columns": [c.strip() for c in r[2].split(",") if c.strip()],
                    "source_exec_sql": r[3],
                    "column_mapping": r[4],
                    "load_option": r[5].strip().lower() if r[5] else "di",
                    "stg_drop_yn": r[6].strip().upper() if r[6] else "N",
                }
            )

        return configs

    @task
    def run_etl(table_config: dict):
        source_table = table_config["source_table"]
        target_table = table_config["target_table"]
        pk_columns = table_config["pk_columns"]
        source_exec_sql = table_config["source_exec_sql"]
        raw_column_mapping = table_config["column_mapping"]
        load_option = table_config["load_option"]
        stg_drop_yn = table_config["stg_drop_yn"]

        stg_table = f"stg_{target_table}"

        create_stg_sql = f"""
            CREATE TABLE IF NOT EXISTS {stg_table}
            (LIKE {target_table} INCLUDING ALL)
        """

        truncate_stg_sql = f"TRUNCATE TABLE {stg_table}"
        truncate_target_sql = f"TRUNCATE TABLE {target_table}"
        drop_stg_sql = f"DROP TABLE IF EXISTS {stg_table}"

        source_hook = PostgresHook(postgres_conn_id=SOURCE_POSTGRES_CONN_ID)
        target_hook = PostgresHook(postgres_conn_id=TARGET_POSTGRES_CONN_ID)

        source_conn = source_hook.get_conn()
        source_cursor = source_conn.cursor(name=f"csr_{target_table}")
        source_cursor.itersize = CHUNK_SIZE

        total_rows = 0

        try:
            # 1) STG 생성 및 비우기
            target_hook.run(create_stg_sql)
            target_hook.run(truncate_stg_sql)

            # 2) source_exec_sql 실행
            source_cursor.execute(source_exec_sql)

            # 3) source 결과 컬럼명 추출
            source_columns = [desc[0] for desc in source_cursor.description]

            if not source_columns:
                raise ValueError(
                    f"{target_table}: source_exec_sql returned no columns"
                )

            # 4) 컬럼 매핑 파싱
            column_mapping = parse_column_mapping(raw_column_mapping)

            # 5) source alias -> target column 변환
            #    매핑이 없으면 동일명 사용
            target_columns = [
                column_mapping.get(src_col, src_col)
                for src_col in source_columns
            ]

            # 중복 target 컬럼 방지
            if len(set(target_columns)) != len(target_columns):
                raise ValueError(
                    f"{target_table}: duplicate target columns detected after mapping. "
                    f"source_columns={source_columns}, target_columns={target_columns}"
                )

            # PK 검증 (pk는 target 기준 컬럼명이라고 가정)
            missing_pk_columns = [pk for pk in pk_columns if pk not in target_columns]
            if missing_pk_columns:
                raise ValueError(
                    f"{target_table}: mapped result does not include PK columns: "
                    f"{missing_pk_columns}. "
                    f"source_columns={source_columns}, target_columns={target_columns}"
                )

            insert_columns_sql = ", ".join(target_columns)
            select_columns_sql = ", ".join([f"s.{col}" for col in target_columns])

            insert_sql = f"""
                INSERT INTO {target_table} ({insert_columns_sql})
                SELECT {select_columns_sql}
                FROM {stg_table} s
            """

            pk_join_condition_sql = " AND ".join(
                [f"t.{pk} = s.{pk}" for pk in pk_columns]
            )

            non_pk_columns = [c for c in target_columns if c not in pk_columns]

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

            delete_sql = f"""
                DELETE FROM {target_table} t
                WHERE EXISTS (
                    SELECT 1
                    FROM {stg_table} s
                    WHERE {pk_join_condition_sql}
                )
            """

            # 6) STG 적재
            # rows 값 순서는 source_columns 순서대로 오므로
            # target_fields만 mapped target_columns로 지정하면 됨
            while True:
                rows = source_cursor.fetchmany(size=CHUNK_SIZE)

                if not rows:
                    break

                target_hook.insert_rows(
                    table=stg_table,
                    rows=rows,
                    target_fields=target_columns,
                    commit_every=CHUNK_SIZE,
                    executemany=True,
                )

                total_rows += len(rows)

                print(
                    f"{source_table} -> {stg_table} "
                    f"chunk={len(rows)} total={total_rows} "
                    f"source_columns={source_columns} target_columns={target_columns}"
                )

            # 7) load_option 별 target 적재
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
                print(f"{source_table}: no rows fetched")

        finally:
            source_cursor.close()
            source_conn.close()

            if stg_drop_yn == "Y":
                target_hook.run(drop_stg_sql)
                print(f"{stg_table} dropped (stg_drop_yn=Y)")
            else:
                print(f"{stg_table} kept (stg_drop_yn=N)")

    table_configs = get_table_configs()
    run_etl.expand(table_config=table_configs)