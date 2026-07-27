from __future__ import annotations

import json
import traceback

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.etl_hist_utils import (
    get_task_runtime_info,
    insert_etl_run_hist,
    update_etl_run_hist_success,
    update_etl_run_hist_failed,
)


META_POSTGRES_CONN_ID = "postgres_conn"
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


def parse_config_option(raw_config_option: str | None) -> dict[str, str]:
    if raw_config_option is None:
        return {}

    raw_config_option = raw_config_option.strip()
    if not raw_config_option:
        return {}

    try:
        parsed = json.loads(raw_config_option)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Invalid config_option JSON: {raw_config_option}"
        ) from e

    if not isinstance(parsed, dict):
        raise ValueError(
            f"config_option must be JSON object(dict): {raw_config_option}"
        )

    result = {}
    for k, v in parsed.items():
        key = str(k).strip()
        val = "" if v is None else str(v).strip()

        if not key:
            raise ValueError(f"Invalid config_option key: {k}")

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


def get_single_table_config(
    dag_id: str,
    task_name: str,
    meta_postgres_conn_id: str = META_POSTGRES_CONN_ID,
) -> dict:
    """
    dag_id + task_name 기준으로 ETL 메타 1건만 조회한다.
    dynamic expand용 list가 아니라 static task용 dict 1건을 반환한다.
    """

    meta_hook = PostgresHook(postgres_conn_id=meta_postgres_conn_id)

    update_input_param_sql = """
    UPDATE etl_meta_db_to_db a
    SET input_param = b.tobe_param
    FROM (
        SELECT dag_id, task_name, tobe_param
        FROM etl_param a
        WHERE dag_id = %s
          AND task_name = %s
          AND (task_name, created_tm) = (
                SELECT b.task_name, b.created_tm
                FROM (
                    SELECT dag_id, task_name, created_tm
                    FROM etl_param
                    WHERE dag_id = a.dag_id
                      AND task_name = a.task_name
                    ORDER BY created_tm DESC
                ) b
                LIMIT 1
          )
    ) b
    WHERE a.dag_id = b.dag_id
      AND a.task_name = b.task_name
    """

    select_meta_sql = """
    SELECT
        task_name,
        COALESCE(exec_seq, 999999) AS exec_seq,
        source_table,
        target_table,
        pk_column,
        source_exec_sql,
        column_mapping,
        load_option,
        stg_drop_yn,
        target_pre_sql,
        target_post_sql,
        config_option,
        input_param
    FROM etl_meta_db_to_db
    WHERE enable_yn = 'Y'
      AND dag_id = %s
      AND task_name = %s
      AND disable_dt = '20991231'
    """

    conn = None
    cursor = None

    try:
        conn = meta_hook.get_conn()
        conn.autocommit = False
        cursor = conn.cursor()

        cursor.execute("SET TIME ZONE 'Asia/Seoul'")

        cursor.execute(update_input_param_sql, (dag_id, task_name))
        cursor.execute(select_meta_sql, (dag_id, task_name))
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

    if not rows:
        raise ValueError(
            f"ETL meta not found. dag_id={dag_id}, task_name={task_name}"
        )

    if len(rows) > 1:
        raise ValueError(
            f"ETL meta must be unique. dag_id={dag_id}, task_name={task_name}, count={len(rows)}"
        )

    r = rows[0]

    meta_task_name = (r[0] or "").strip() if r[0] is not None else ""
    exec_seq = int(r[1]) if r[1] is not None else 999999

    source_table = (r[2] or "").strip() if r[2] is not None else ""
    target_table = (r[3] or "").strip() if r[3] is not None else ""
    pk_columns = parse_csv_columns(r[4])
    source_exec_sql = (r[5] or "").strip() if r[5] is not None else ""
    column_mapping = r[6]
    load_option = (r[7] or "di").strip().lower() if r[7] is not None else "di"
    stg_drop_yn = (r[8] or "N").strip().upper() if r[8] is not None else "N"
    target_pre_sql = (r[9] or "").strip() if r[9] is not None else ""
    target_post_sql = (r[10] or "").strip() if r[10] is not None else ""
    config_option = (r[11] or "").strip() if r[11] is not None else ""
    input_param = (r[12] or "").strip() if r[12] is not None else ""

    if not meta_task_name:
        meta_task_name = task_name

    if not target_table:
        raise ValueError("etl_meta.target_table is empty")

    if not source_exec_sql:
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
            f"{target_table}: pk_column is required for load_option [{load_option}]"
        )

    parsed_config_option = parse_config_option(config_option)

    source_conn_name = (
        parsed_config_option.get("SOURCE_CONN_NAME") or ""
    ).strip()

    target_conn_name = (
        parsed_config_option.get("TARGET_CONN_NAME") or ""
    ).strip()

    if not source_conn_name:
        raise ValueError(
            f"{target_table}: config_option.SOURCE_CONN_NAME is empty"
        )

    if not target_conn_name:
        raise ValueError(
            f"{target_table}: config_option.TARGET_CONN_NAME is empty"
        )

    return {
        "task_name": meta_task_name,
        "exec_seq": exec_seq,
        "source_table": source_table,
        "target_table": target_table,
        "pk_columns": pk_columns,
        "source_exec_sql": source_exec_sql,
        "column_mapping": column_mapping,
        "load_option": load_option,
        "stg_drop_yn": stg_drop_yn,
        "target_pre_sql": target_pre_sql,
        "target_post_sql": target_post_sql,
        "config_option": parsed_config_option,
        "input_param": input_param,
        "source_conn_name": source_conn_name,
        "target_conn_name": target_conn_name,
    }


def run_postgres_to_postgres_etl(
    dag_id: str,
    task_name: str,
    meta_postgres_conn_id: str = META_POSTGRES_CONN_ID,
    chunk_size: int = CHUNK_SIZE,
    **context,
):
    """
    static task에서 직접 호출되는 실제 ETL 공통 함수.
    dag_id + task_name 기준으로 메타 1건 조회 후 ETL 수행.
    """

    runtime_info = get_task_runtime_info(**context)

    table_config = get_single_table_config(
        dag_id=dag_id,
        task_name=task_name,
        meta_postgres_conn_id=meta_postgres_conn_id,
    )

    meta_task_name = (table_config.get("task_name") or "").strip()
    exec_seq = table_config.get("exec_seq")

    source_table = (table_config.get("source_table") or "").strip()
    target_table = (table_config.get("target_table") or "").strip()
    pk_columns = table_config.get("pk_columns") or []
    raw_source_exec_sql = (table_config.get("source_exec_sql") or "").strip()
    raw_column_mapping = table_config.get("column_mapping")
    load_option = (table_config.get("load_option") or "di").strip().lower()
    stg_drop_yn = (table_config.get("stg_drop_yn") or "N").strip().upper()
    raw_target_pre_sql = (table_config.get("target_pre_sql") or "").strip()
    raw_target_post_sql = (table_config.get("target_post_sql") or "").strip()
    config_option = table_config.get("config_option") or {}
    raw_input_param = table_config.get("input_param")
    source_conn_name = (table_config.get("source_conn_name") or "").strip()
    target_conn_name = (table_config.get("target_conn_name") or "").strip()

    print(
        f"START ETL "
        f"dag_id={dag_id}, "
        f"task_name={meta_task_name}, "
        f"exec_seq={exec_seq}, "
        f"source_table={source_table}, "
        f"target_table={target_table}"
    )

    run_hist_id = None
    extract_row_count = 0
    stg_load_row_count = 0
    target_insert_count = 0
    target_update_count = 0
    target_delete_count = 0
    file_write_row_count = 0
    target_file_path = ""

    try:
        run_hist_id = insert_etl_run_hist(
            dag_id=dag_id,
            run_id=runtime_info["run_id"],
            task_id=meta_task_name or runtime_info["task_id"],
            map_index=runtime_info["map_index"],
            source_table=source_table,
            target_table=target_table,
            load_option=load_option,
            source_conn_name=source_conn_name,
            target_conn_name=target_conn_name,
            input_param=raw_input_param,
            config_option={
                **config_option,
                "AIRFLOW_TASK_ID": runtime_info["task_id"],
                "TASK_NAME": meta_task_name,
                "EXEC_SEQ": str(exec_seq),
            },
        )

        input_params = parse_input_params(raw_input_param)

        source_exec_sql = apply_input_params(
            raw_source_exec_sql,
            input_params,
        )

        target_pre_sql = apply_input_params(
            raw_target_pre_sql,
            input_params,
        )

        target_post_sql = apply_input_params(
            raw_target_post_sql,
            input_params,
        )

        stg_table = f"stg_{target_table}"

        create_stg_sql = f"""
            CREATE TABLE IF NOT EXISTS {stg_table}
            (LIKE {target_table} INCLUDING ALL)
        """

        truncate_stg_sql = f"TRUNCATE TABLE {stg_table}"
        truncate_target_sql = f"TRUNCATE TABLE {target_table}"
        drop_stg_sql = f"DROP TABLE IF EXISTS {stg_table}"

        source_hook = PostgresHook(postgres_conn_id=source_conn_name)
        target_hook = PostgresHook(postgres_conn_id=target_conn_name)

        source_conn = None
        meta_cursor = None
        source_cursor = None
        target_tx_conn = None
        target_tx_cursor = None

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
                    f"{target_table}: source_exec_sql returned no columns."
                )

            column_mapping = parse_column_mapping(raw_column_mapping) or {}

            target_columns = [
                column_mapping.get(src_col, src_col)
                for src_col in source_columns
            ]

            if len(set(target_columns)) != len(target_columns):
                raise ValueError(
                    f"{target_table}: duplicate target columns detected after mapping. "
                    f"source_columns={source_columns}, target_columns={target_columns}"
                )

            missing_pk_columns = [
                pk for pk in pk_columns if pk not in target_columns
            ]

            if missing_pk_columns:
                raise ValueError(
                    f"{target_table}: mapped result does not include PK columns: "
                    f"{missing_pk_columns}"
                )

            insert_columns_sql = ", ".join(target_columns)
            select_columns_sql = ", ".join(
                [f"s.{col}" for col in target_columns]
            )

            insert_sql = f"""
                INSERT INTO {target_table} ({insert_columns_sql})
                SELECT {select_columns_sql}
                FROM {stg_table} s
            """

            pk_join_condition_sql = " AND ".join(
                [f"t.{pk} = s.{pk}" for pk in pk_columns]
            )

            non_pk_columns = [
                c for c in target_columns if c not in pk_columns
            ]

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
            source_cursor.itersize = chunk_size
            source_cursor.execute(source_exec_sql)

            while True:
                rows = source_cursor.fetchmany(size=chunk_size)

                if not rows:
                    break

                target_hook.insert_rows(
                    table=stg_table,
                    rows=rows,
                    target_fields=target_columns,
                    commit_every=chunk_size,
                    executemany=True,
                )

                extract_row_count += len(rows)
                stg_load_row_count += len(rows)

                print(
                    f"dag_id={dag_id}, "
                    f"task_name={meta_task_name}, "
                    f"exec_seq={exec_seq}, "
                    f"{source_table or '[source_sql]'} -> {stg_table} "
                    f"chunk={len(rows)} total={stg_load_row_count}"
                )

            if stg_load_row_count > 0:
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
                            target_update_count = target_tx_cursor.rowcount
                            print(f"{target_table} UPDATE completed")

                        target_tx_cursor.execute(insert_not_exists_sql)
                        target_insert_count = target_tx_cursor.rowcount
                        print(f"{target_table} INSERT completed (UI)")

                    elif load_option == "di":
                        target_tx_cursor.execute(delete_sql)
                        target_delete_count = target_tx_cursor.rowcount
                        print(f"{target_table} DELETE completed")

                        target_tx_cursor.execute(insert_sql)
                        target_insert_count = target_tx_cursor.rowcount
                        print(f"{target_table} INSERT completed (DI)")

                    elif load_option == "ti":
                        target_tx_cursor.execute(truncate_target_sql)
                        print(f"{target_table} TRUNCATE completed")

                        target_tx_cursor.execute(insert_sql)
                        target_insert_count = target_tx_cursor.rowcount
                        print(f"{target_table} INSERT completed (TI)")

                    elif load_option == "i":
                        target_tx_cursor.execute(insert_sql)
                        target_insert_count = target_tx_cursor.rowcount
                        print(f"{target_table} INSERT completed (I)")

                    elif load_option == "u":
                        if update_sql:
                            target_tx_cursor.execute(update_sql)
                            target_update_count = target_tx_cursor.rowcount
                            print(f"{target_table} UPDATE completed (U)")
                        else:
                            print(
                                f"{target_table}: no non-pk columns to update, UPDATE skipped"
                            )

                    elif load_option == "d":
                        target_tx_cursor.execute(delete_sql)
                        target_delete_count = target_tx_cursor.rowcount
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
                print(f"{stg_table} dropped")
            else:
                print(
                    f"{stg_table} kept "
                    f"(job_succeeded={job_succeeded}, stg_drop_yn={stg_drop_yn})"
                )

        update_etl_run_hist_success(
            run_hist_id=run_hist_id,
            extract_row_count=extract_row_count,
            stg_load_row_count=stg_load_row_count,
            target_insert_count=target_insert_count,
            target_update_count=target_update_count,
            target_delete_count=target_delete_count,
            file_write_row_count=file_write_row_count,
            target_file_path=target_file_path,
        )

        print(
            f"END ETL SUCCESS "
            f"dag_id={dag_id}, "
            f"task_name={meta_task_name}, "
            f"exec_seq={exec_seq}, "
            f"source_table={source_table}, "
            f"target_table={target_table}"
        )

    except Exception:
        print(
            f"END ETL FAILED "
            f"dag_id={dag_id}, "
            f"task_name={task_name}, "
            f"source_table={source_table}, "
            f"target_table={target_table}"
        )

        if run_hist_id is not None:
            update_etl_run_hist_failed(
                run_hist_id=run_hist_id,
                error_message=traceback.format_exc(),
                extract_row_count=extract_row_count,
                stg_load_row_count=stg_load_row_count,
                target_insert_count=target_insert_count,
                target_update_count=target_update_count,
                target_delete_count=target_delete_count,
                file_write_row_count=file_write_row_count,
                target_file_path=target_file_path,
            )

        raise


def create_postgres_to_postgres_task(
    dag_id: str,
    task_name: str,
    task_id: str | None = None,
    meta_postgres_conn_id: str = META_POSTGRES_CONN_ID,
    chunk_size: int = CHUNK_SIZE,
):
    """
    static Airflow task 생성 함수.

    task_name:
        etl_meta_db_to_db.task_name 값

    task_id:
        Airflow UI에 표시될 task_id.
        생략하면 task_name을 그대로 사용한다.
    """

    airflow_task_id = task_id or task_name

    @task(task_id=airflow_task_id)
    def _static_etl_task(**context):
        run_postgres_to_postgres_etl(
            dag_id=dag_id,
            task_name=task_name,
            meta_postgres_conn_id=meta_postgres_conn_id,
            chunk_size=chunk_size,
            **context,
        )

    return _static_etl_task()