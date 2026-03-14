from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "dynamic_postgres_to_postgres_etl_meta_2"

SOURCE_POSTGRES_CONN_ID = "postgres_conn"
TARGET_POSTGRES_CONN_ID = "postgres_conn"

CHUNK_SIZE = 5000


def parse_csv_columns(raw_value: str | None) -> list[str]:
    if raw_value is None:
        return []

    return [c.strip() for c in raw_value.split(",") if c and c.strip()]


def parse_column_mapping(raw_mapping: str | None) -> dict[str, str]:
    if raw_mapping is None:
        return {}

    raw_mapping = raw_mapping.strip()
    if not raw_mapping:
        return {}

    try:
        parsed = json.loads(raw_mapping)

        if isinstance(parsed, dict):
            result = {}
            for k, v in parsed.items():
                src = str(k).strip()
                tgt = str(v).strip()

                if not src or not tgt:
                    raise ValueError(
                        f"Invalid column_mapping JSON entry: {k}:{v}"
                    )

                result[src] = tgt

            return result

        if isinstance(parsed, list):
            result = {}
            for item in parsed:
                if not isinstance(item, dict):
                    raise ValueError(
                        f"Invalid column_mapping JSON list item: {item}"
                    )

                if "source" not in item or "target" not in item:
                    raise ValueError(
                        f"Invalid column_mapping JSON list item: {item}"
                    )

                src = str(item["source"]).strip()
                tgt = str(item["target"]).strip()

                if not src or not tgt:
                    raise ValueError(
                        f"Invalid column_mapping JSON list item: {item}"
                    )

                result[src] = tgt

            return result

    except json.JSONDecodeError:
        pass

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


def parse_input_params(raw_input_param: str | None) -> dict[str, str]:
    if raw_input_param is None:
        return {}

    raw_input_param = raw_input_param.strip()
    if not raw_input_param:
        return {}

    try:
        parsed = json.loads(raw_input_param)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid input_param JSON: {raw_input_param}") from e

    if not isinstance(parsed, dict):
        raise ValueError(
            f"input_param must be JSON object(dict): {raw_input_param}"
        )

    result = {}
    for k, v in parsed.items():
        key = str(k).strip()
        val = "" if v is None else str(v)

        if not key:
            raise ValueError(f"Invalid input_param key: {k}")

        result[key] = val

    return result


def apply_input_params(sql_text: str | None, input_params: dict[str, str]) -> str:
    if sql_text is None:
        return ""

    result = sql_text.strip()
    if not result:
        return ""

    if not input_params:
        return result

    for key in sorted(input_params.keys(), key=len, reverse=True):
        result = result.replace(key, input_params[key])

    return result


def build_limit_0_sql(source_exec_sql: str) -> str:
    return f"""
        SELECT *
        FROM (
            {source_exec_sql}
        ) q
        LIMIT 0
    """


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
) as dag:

    @task
    def get_table_configs():
        source_hook = PostgresHook(postgres_conn_id=SOURCE_POSTGRES_CONN_ID)

        update_input_param_sql = """
        UPDATE etl_meta a
        SET input_param = b.tobe_param
        FROM (
            SELECT dag_id, tobe_param
            FROM etl_param
            WHERE dag_id = %s
            ORDER BY created_tm DESC
            LIMIT 1
        ) b
        WHERE a.dag_id = b.dag_id
        """

        select_meta_sql = """
        SELECT
            source_table,
            target_table,
            pk_column,
            source_exec_sql,
            column_mapping,
            load_option,
            stg_drop_yn,
            target_pre_sql,
            target_post_sql,
            input_param
        FROM etl_meta
        WHERE 1=1
          AND enable_yn = 'Y'
          AND job_type = 'DAILY'
          AND dag_id = %s
        """

        conn = None
        cursor = None

        try:
            conn = source_hook.get_conn()
            conn.autocommit = False
            cursor = conn.cursor()

            # 1) 현재 DAG_ID 기준 최신 tobe_param 으로 etl_meta.input_param 업데이트
            cursor.execute(update_input_param_sql, (DAG_ID,))

            # 2) 업데이트 후 etl_meta 조회
            cursor.execute(select_meta_sql, (DAG_ID,))
            rows = cursor.fetchall()

            conn.commit()

        except Exception:
            if conn is not None:
                conn.rollback()
            raise

        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

        configs = []

        for r in rows:
            source_table = (r[0] or "").strip() if r[0] is not None else ""
            target_table = (r[1] or "").strip() if r[1] is not None else ""
            pk_columns = parse_csv_columns(r[2])
            source_exec_sql = (r[3] or "").strip() if r[3] is not None else ""
            column_mapping = r[4]
            load_option = (r[5] or "di").strip().lower() if r[5] is not None else "di"
            stg_drop_yn = (r[6] or "N").strip().upper() if r[6] is not None else "N"
            target_pre_sql = (r[7] or "").strip() if r[7] is not None else ""
            target_post_sql = (r[8] or "").strip() if r[8] is not None else ""
            input_param = (r[9] or "").strip() if r[9] is not None else ""

            if not target_table:
                raise ValueError("etl_meta.target_table is empty")

            if load_option not in ("ui", "di", "ti", "i", "u", "d"):
                raise ValueError(
                    f"{target_table}: invalid load_option [{load_option}]. "
                    f"Allowed values are ui, di, ti, i, u, d."
                )

            if stg_drop_yn not in ("Y", "N"):
                raise ValueError(
                    f"{target_table}: invalid stg_drop_yn [{stg_drop_yn}]. "
                    f"Allowed values are Y, N."
                )

            if not source_exec_sql:
                raise ValueError(
                    f"{target_table}: source_exec_sql is empty"
                )

            if load_option in ("ui", "di", "u", "d") and not pk_columns:
                raise ValueError(
                    f"{target_table}: pk_column is required for load_option [{load_option}]"
                )

            configs.append(
                {
                    "source_table": source_table,
                    "target_table": target_table,
                    "pk_columns": pk_columns,
                    "source_exec_sql": source_exec_sql,
                    "column_mapping": column_mapping,
                    "load_option": load_option,
                    "stg_drop_yn": stg_drop_yn,
                    "target_pre_sql": target_pre_sql,
                    "target_post_sql": target_post_sql,
                    "input_param": input_param,
                }
            )

        return configs

    @task(pool_slots=1)
    def run_etl(table_config: dict):
        source_table = (table_config.get("source_table") or "").strip()
        target_table = (table_config.get("target_table") or "").strip()
        pk_columns = table_config.get("pk_columns") or []
        raw_source_exec_sql = (table_config.get("source_exec_sql") or "").strip()
        raw_column_mapping = table_config.get("column_mapping")
        load_option = (table_config.get("load_option") or "di").strip().lower()
        stg_drop_yn = (table_config.get("stg_drop_yn") or "N").strip().upper()
        raw_target_pre_sql = (table_config.get("target_pre_sql") or "").strip()
        raw_target_post_sql = (table_config.get("target_post_sql") or "").strip()
        raw_input_param = table_config.get("input_param")

        if not target_table:
            raise ValueError("table_config.target_table is empty")

        if not raw_source_exec_sql:
            raise ValueError(f"{target_table}: source_exec_sql is empty")

        if load_option not in ("ui", "di", "ti", "i", "u", "d"):
            raise ValueError(
                f"{target_table}: invalid load_option [{load_option}]"
            )

        if stg_drop_yn not in ("Y", "N"):
            raise ValueError(
                f"{target_table}: invalid stg_drop_yn [{stg_drop_yn}]"
            )

        if load_option in ("ui", "di", "u", "d") and not pk_columns:
            raise ValueError(
                f"{target_table}: pk_columns is required for load_option [{load_option}]"
            )

        input_params = parse_input_params(raw_input_param)

        source_exec_sql = apply_input_params(raw_source_exec_sql, input_params)
        target_pre_sql = apply_input_params(raw_target_pre_sql, input_params)
        target_post_sql = apply_input_params(raw_target_post_sql, input_params)

        if not source_exec_sql:
            raise ValueError(f"{target_table}: source_exec_sql is empty after param replacement")

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

        source_conn = None
        meta_cursor = None
        source_cursor = None

        target_tx_conn = None
        target_tx_cursor = None

        total_rows = 0
        job_succeeded = False

        try:
            target_hook.run(create_stg_sql)
            target_hook.run(truncate_stg_sql)

            source_conn = source_hook.get_conn()

            meta_cursor = source_conn.cursor()
            meta_sql = build_limit_0_sql(source_exec_sql)
            meta_cursor.execute(meta_sql)

            if meta_cursor.description is None:
                raise ValueError(
                    f"{target_table}: source_exec_sql did not return a result set. "
                    f"Only SELECT query is allowed. source_exec_sql=[{source_exec_sql}]"
                )

            source_columns = [desc[0] for desc in meta_cursor.description]

            if not source_columns:
                raise ValueError(
                    f"{target_table}: source_exec_sql returned no columns. "
                    f"source_exec_sql=[{source_exec_sql}]"
                )

            column_mapping = parse_column_mapping(raw_column_mapping) or {}

            target_columns = [
                column_mapping.get(src_col, src_col)
                for src_col in source_columns
            ]

            if not target_columns:
                raise ValueError(
                    f"{target_table}: mapped target_columns is empty"
                )

            if len(set(target_columns)) != len(target_columns):
                raise ValueError(
                    f"{target_table}: duplicate target columns detected after mapping. "
                    f"source_columns={source_columns}, target_columns={target_columns}"
                )

            missing_pk_columns = [pk for pk in pk_columns if pk not in target_columns]
            if missing_pk_columns:
                raise ValueError(
                    f"{target_table}: mapped result does not include PK columns: "
                    f"{missing_pk_columns}. "
                    f"source_columns={source_columns}, "
                    f"target_columns={target_columns}"
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

            source_cursor = source_conn.cursor(name=f"csr_{target_table}")
            source_cursor.itersize = CHUNK_SIZE
            source_cursor.execute(source_exec_sql)

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
                    f"{source_table or '[source_sql]'} -> {stg_table} "
                    f"chunk={len(rows)} total={total_rows} "
                    f"source_columns={source_columns} "
                    f"target_columns={target_columns}"
                )

            if total_rows > 0:
                target_tx_conn = target_hook.get_conn()
                target_tx_conn.autocommit = False
                target_tx_cursor = target_tx_conn.cursor()

                try:
                    if target_pre_sql:
                        target_tx_cursor.execute(target_pre_sql)
                        print(f"{target_table} target_pre_sql completed")

                    if load_option == "ui":
                        if update_sql:
                            target_tx_cursor.execute(update_sql)
                            print(f"{target_table} UPDATE completed")

                        target_tx_cursor.execute(insert_not_exists_sql)
                        print(
                            f"{stg_table} -> {target_table} INSERT completed (UI), total={total_rows}"
                        )

                    elif load_option == "di":
                        target_tx_cursor.execute(delete_sql)
                        print(f"{target_table} DELETE completed")

                        target_tx_cursor.execute(insert_sql)
                        print(
                            f"{stg_table} -> {target_table} INSERT completed (DI), total={total_rows}"
                        )

                    elif load_option == "ti":
                        target_tx_cursor.execute(truncate_target_sql)
                        print(f"{target_table} TRUNCATE completed")

                        target_tx_cursor.execute(insert_sql)
                        print(
                            f"{stg_table} -> {target_table} INSERT completed (TI), total={total_rows}"
                        )

                    elif load_option == "i":
                        target_tx_cursor.execute(insert_sql)
                        print(
                            f"{stg_table} -> {target_table} INSERT completed (I), total={total_rows}"
                        )

                    elif load_option == "u":
                        if not update_sql:
                            print(
                                f"{target_table}: no non-pk columns to update, UPDATE skipped (U)"
                            )
                        else:
                            target_tx_cursor.execute(update_sql)
                            print(f"{target_table} UPDATE completed (U)")

                    elif load_option == "d":
                        target_tx_cursor.execute(delete_sql)
                        print(f"{target_table} DELETE completed (D)")

                    if target_post_sql:
                        target_tx_cursor.execute(target_post_sql)
                        print(f"{target_table} target_post_sql completed")

                    target_tx_conn.commit()
                    job_succeeded = True

                except Exception:
                    target_tx_conn.rollback()
                    raise

            else:
                print(f"{target_table}: no rows fetched, target load skipped")
                job_succeeded = True

        finally:
            if meta_cursor is not None:
                meta_cursor.close()

            if source_cursor is not None:
                source_cursor.close()

            if source_conn is not None:
                source_conn.close()

            if target_tx_cursor is not None:
                target_tx_cursor.close()

            if target_tx_conn is not None:
                target_tx_conn.close()

            if job_succeeded and stg_drop_yn == "Y":
                target_hook.run(drop_stg_sql)
                print(f"{stg_table} dropped (stg_drop_yn=Y)")
            else:
                print(
                    f"{stg_table} kept "
                    f"(job_succeeded={job_succeeded}, stg_drop_yn={stg_drop_yn})"
                )

    table_configs = get_table_configs()
    run_etl.expand(table_config=table_configs)