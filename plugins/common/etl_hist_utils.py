from __future__ import annotations

import json
from airflow.providers.postgres.hooks.postgres import PostgresHook


META_POSTGRES_CONN_ID = "postgres_conn"
HIST_TABLE_NAME = "etl_job_run_dtl_hist"


def safe_json_dumps(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


def cut_text(value: str | None, max_length: int = 4000) -> str:
    if value is None:
        return ""
    text = str(value)
    if len(text) <= max_length:
        return text
    return text[:max_length]


def get_task_runtime_info(**context) -> dict:
    ti = context.get("ti")
    task = context.get("task")

    return {
        "run_id": context.get("run_id"),
        "task_id": getattr(task, "task_id", None),
        "map_index": getattr(ti, "map_index", None),
    }


def insert_etl_run_hist(
    dag_id: str,
    run_id: str | None,
    task_id: str | None,
    map_index: int | None,
    source_table: str | None,
    target_table: str | None,
    load_option: str | None = None,
    source_conn_name: str | None = None,
    target_conn_name: str | None = None,
    input_param=None,
    config_option=None,
    create_user_id: str = "airflow",
    meta_conn_id: str = META_POSTGRES_CONN_ID,
) -> int:
    hook = PostgresHook(postgres_conn_id=meta_conn_id)

    insert_sql = f"""
        INSERT INTO {HIST_TABLE_NAME} (
            dag_id,
            run_id,
            task_id,
            map_index,
            source_table,
            target_table,
            load_option,
            source_conn_name,
            target_conn_name,
            extract_row_count,
            stg_load_row_count,
            target_insert_count,
            target_update_count,
            target_delete_count,
            target_total_affected_count,
            file_write_row_count,
            target_file_path,
            status,
            error_message,
            start_tm,
            end_tm,
            input_param,
            config_option,
            create_user_id,
            create_tm,
            update_tm
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            0, 0, 0, 0, 0, 0, 0, '',
            'RUNNING', '', NOW(), NULL,
            %s, %s, %s, NOW(), NOW()
        )
        RETURNING run_hist_id
    """

    conn = None
    cursor = None
    try:
        conn = hook.get_conn()
        conn.autocommit = False
        cursor = conn.cursor()

        cursor.execute(
            insert_sql,
            (
                dag_id,
                run_id,
                task_id,
                map_index,
                source_table or "",
                target_table or "",
                load_option or "",
                source_conn_name or "",
                target_conn_name or "",
                safe_json_dumps(input_param),
                safe_json_dumps(config_option),
                create_user_id,
            ),
        )
        run_hist_id = cursor.fetchone()[0]
        conn.commit()
        return run_hist_id

    except Exception:
        if conn is not None:
            conn.rollback()
        raise

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def update_etl_run_hist_success(
    run_hist_id: int,
    extract_row_count: int = 0,
    stg_load_row_count: int = 0,
    target_insert_count: int = 0,
    target_update_count: int = 0,
    target_delete_count: int = 0,
    file_write_row_count: int = 0,
    target_file_path: str | None = None,
    meta_conn_id: str = META_POSTGRES_CONN_ID,
) -> None:
    hook = PostgresHook(postgres_conn_id=meta_conn_id)

    total_affected = (
        int(target_insert_count)
        + int(target_update_count)
        + int(target_delete_count)
    )

    update_sql = f"""
        UPDATE {HIST_TABLE_NAME}
           SET extract_row_count = %s,
               stg_load_row_count = %s,
               target_insert_count = %s,
               target_update_count = %s,
               target_delete_count = %s,
               target_total_affected_count = %s,
               file_write_row_count = %s,
               target_file_path = %s,
               status = 'SUCCESS',
               error_message = '',
               end_tm = NOW(),
               update_tm = NOW()
         WHERE run_hist_id = %s
    """

    hook.run(
        update_sql,
        parameters=(
            extract_row_count,
            stg_load_row_count,
            target_insert_count,
            target_update_count,
            target_delete_count,
            total_affected,
            file_write_row_count,
            target_file_path or "",
            run_hist_id,
        ),
    )


def update_etl_run_hist_failed(
    run_hist_id: int,
    error_message: str,
    extract_row_count: int = 0,
    stg_load_row_count: int = 0,
    target_insert_count: int = 0,
    target_update_count: int = 0,
    target_delete_count: int = 0,
    file_write_row_count: int = 0,
    target_file_path: str | None = None,
    meta_conn_id: str = META_POSTGRES_CONN_ID,
) -> None:
    hook = PostgresHook(postgres_conn_id=meta_conn_id)

    total_affected = (
        int(target_insert_count)
        + int(target_update_count)
        + int(target_delete_count)
    )

    update_sql = f"""
        UPDATE {HIST_TABLE_NAME}
           SET extract_row_count = %s,
               stg_load_row_count = %s,
               target_insert_count = %s,
               target_update_count = %s,
               target_delete_count = %s,
               target_total_affected_count = %s,
               file_write_row_count = %s,
               target_file_path = %s,
               status = 'FAILED',
               error_message = %s,
               end_tm = NOW(),
               update_tm = NOW()
         WHERE run_hist_id = %s
    """

    hook.run(
        update_sql,
        parameters=(
            extract_row_count,
            stg_load_row_count,
            target_insert_count,
            target_update_count,
            target_delete_count,
            total_affected,
            file_write_row_count,
            target_file_path or "",
            cut_text(error_message, 4000),
            run_hist_id,
        ),
    )