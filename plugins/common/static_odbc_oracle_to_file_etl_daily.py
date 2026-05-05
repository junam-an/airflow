from __future__ import annotations

import re
import csv
import json
import subprocess
import traceback
from pathlib import Path

from airflow.decorators import task
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


DEFAULT_META_POSTGRES_CONN_ID = "postgres_conn"

from common.etl_hist_utils import (
    get_task_runtime_info,
    insert_etl_run_hist,
    update_etl_run_hist_success,
    update_etl_run_hist_failed,
)


def normalize_airflow_task_id(value: str) -> str:
    task_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", value.strip())

    if not task_id:
        raise ValueError("airflow task_id is empty")

    return task_id

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
        key = str(k).strip().upper()
        val = "" if v is None else str(v).strip()

        if not key:
            raise ValueError(f"Invalid config_option key: {k}")

        result[key] = val

    return result


def apply_input_params(text: str | None, input_params: dict[str, str]) -> str:
    """
    SQL 용 치환.
    예: $$P_BASE_DT -> '20260314'
    """
    if text is None:
        return ""

    result = text.strip()

    if not result:
        return ""

    if not input_params:
        return result

    for key in sorted(input_params.keys(), key=len, reverse=True):
        result = result.replace(key, input_params[key])

    return result


def strip_outer_single_quotes(value: str) -> str:
    result = str(value).strip()

    if len(result) >= 2 and result[0] == "'" and result[-1] == "'":
        return result[1:-1]

    return result.replace("'", "")


def apply_input_params_for_file(
    text: str | None,
    input_params: dict[str, str],
) -> str:
    """
    파일명, 디렉터리, OS command 용 치환.
    작은따옴표를 제거한 값으로 치환한다.
    예: $$P_BASE_DT -> 20260314
    """
    if text is None:
        return ""

    result = text.strip()

    if not result:
        return ""

    if not input_params:
        return result

    normalized_params = {
        key: strip_outer_single_quotes(val)
        for key, val in input_params.items()
    }

    for key in sorted(normalized_params.keys(), key=len, reverse=True):
        result = result.replace(key, normalized_params[key])

    return result


def normalize_csv_delimiter(raw_delimiter: str | None) -> str:
    if raw_delimiter is None:
        return ","

    delimiter = str(raw_delimiter).strip()

    if not delimiter:
        return ","

    if delimiter in ("\\t", "tab", "TAB"):
        return "\t"

    return delimiter


def normalize_file_encoding(raw_encoding: str | None) -> str:
    if raw_encoding is None:
        return "utf-8"

    encoding = str(raw_encoding).strip().lower()

    if not encoding:
        return "utf-8"

    if encoding in ("utf8", "utf-8"):
        return "utf-8"

    if encoding in ("euckr", "euc-kr"):
        return "euc-kr"

    if encoding in ("cp949", "ms949"):
        return "cp949"

    raise ValueError(
        f"Unsupported target_file_encoding: {raw_encoding}. "
        f"Allowed values are utf-8, euc-kr, cp949."
    )


def build_limit_0_sql_for_oracle(source_exec_sql: str) -> str:
    return f"""
        SELECT *
        FROM (
            {source_exec_sql}
        ) q
        WHERE 1 = 0
    """


def write_csv_file(
    file_path: str,
    columns: list[str],
    rows: list[tuple],
    delimiter: str,
    encoding: str,
) -> None:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding=encoding, newline="") as f:
        writer = csv.writer(f, delimiter=delimiter)
        writer.writerow(columns)
        writer.writerows(rows)


def write_json_file(
    file_path: str,
    columns: list[str],
    rows: list[tuple],
    encoding: str,
) -> None:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    data = [
        {columns[idx]: row[idx] for idx in range(len(columns))}
        for row in rows
    ]

    with path.open("w", encoding=encoding) as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def write_text_file(
    file_path: str,
    columns: list[str],
    rows: list[tuple],
    encoding: str,
) -> None:
    if len(columns) != 1:
        raise ValueError(
            f"text target requires exactly one output column. columns={columns}"
        )

    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding=encoding, newline="") as f:
        for row in rows:
            value = "" if row[0] is None else str(row[0])
            f.write(value)
            f.write("\n")

def get_odbc_to_file_task_config(
    dag_id: str,
    task_name: str,
    meta_postgres_conn_id: str = DEFAULT_META_POSTGRES_CONN_ID,
) -> dict:
    """
    dag_id + task_name 기준으로 etl_meta_db_to_file 1건만 조회한다.
    """

    meta_hook = PostgresHook(postgres_conn_id=meta_postgres_conn_id)

    update_input_param_sql = """
    UPDATE etl_meta_db_to_file a
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
        source_exec_sql,
        column_mapping,
        target_file_type,
        csv_file_delimiter,
        target_file_encoding,
        target_file_dir,
        target_pre_cmd,
        target_post_cmd,
        config_option,
        input_param
    FROM etl_meta_db_to_file
    WHERE 1=1
      AND enable_yn = 'Y'
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
            f"etl_meta_db_to_file config not found. "
            f"dag_id={dag_id}, task_name={task_name}"
        )

    if len(rows) > 1:
        raise ValueError(
            f"etl_meta_db_to_file config must be one row. "
            f"dag_id={dag_id}, task_name={task_name}, count={len(rows)}"
        )

    r = rows[0]

    meta_task_name = (r[0] or "").strip()
    exec_seq = int(r[1]) if r[1] is not None else 999999
    source_table = (r[2] or "").strip()
    target_table = (r[3] or "").strip()
    source_exec_sql = (r[4] or "").strip()
    column_mapping = r[5]

    target_file_type = (r[6] or "").strip().lower()
    csv_file_delimiter = r[7] or ","
    target_file_encoding = (r[8] or "utf-8").strip()
    target_file_dir = (r[9] or "").strip()
    target_pre_cmd = (r[10] or "").strip()
    target_post_cmd = (r[11] or "").strip()
    input_param = (r[12] or "").strip()
    config_option = (r[13] or "").strip()

    parsed_config_option = parse_config_option(config_option)

    source_conn_name = (
        parsed_config_option.get("SOURCE_CONN_NAME") or ""
    ).strip()

    if not meta_task_name:
        meta_task_name = task_name

    if not source_table:
        raise ValueError(f"{task_name}: source_table is empty")

    if not target_table:
        raise ValueError(f"{task_name}: target_table is empty")

    if not source_exec_sql:
        raise ValueError(f"{task_name}: source_exec_sql is empty")

    if target_file_type not in ("json", "csv", "text"):
        raise ValueError(
            f"{task_name}: invalid target_file_type [{target_file_type}]. "
            f"Allowed values are json, csv, text."
        )

    if target_file_type == "csv" and not csv_file_delimiter:
        raise ValueError(f"{task_name}: csv_file_delimiter is empty")

    if not target_file_dir:
        raise ValueError(f"{task_name}: target_file_dir is empty")

    if not source_conn_name:
        raise ValueError(
            f"{task_name}: config_option.SOURCE_CONN_NAME is empty"
        )

    return {
        "task_name": meta_task_name,
        "exec_seq": exec_seq,
        "source_table": source_table,
        "target_table": target_table,
        "source_exec_sql": source_exec_sql,
        "column_mapping": column_mapping,
        "target_file_type": target_file_type,
        "csv_file_delimiter": csv_file_delimiter,
        "target_file_encoding": normalize_file_encoding(target_file_encoding),
        "target_file_dir": target_file_dir,
        "target_pre_cmd": target_pre_cmd,
        "target_post_cmd": target_post_cmd,
        "input_param": input_param,
        "config_option": parsed_config_option,
        "source_conn_name": source_conn_name,
    }


def run_odbc_to_file_common(
    dag_id: str,
    task_name: str,
    meta_postgres_conn_id: str = DEFAULT_META_POSTGRES_CONN_ID,
    **context,
) -> None:
    """
    실제 ODBC DB -> File ETL 공통 실행 함수.
    Airflow task 함수가 아니라 순수 Python 공통 함수다.
    """

    table_config = get_odbc_to_file_task_config(
        dag_id=dag_id,
        task_name=task_name,
        meta_postgres_conn_id=meta_postgres_conn_id,
    )

    runtime_info = get_task_runtime_info(**context)

    meta_task_name = (table_config.get("task_name") or "").strip()
    exec_seq = table_config.get("exec_seq")

    source_table = (table_config.get("source_table") or "").strip()
    raw_target_file_name = (table_config.get("target_table") or "").strip()
    raw_source_exec_sql = (table_config.get("source_exec_sql") or "").strip()
    raw_column_mapping = table_config.get("column_mapping")

    raw_target_file_type = (
        table_config.get("target_file_type") or ""
    ).strip().lower()

    raw_csv_file_delimiter = str(
        table_config.get("csv_file_delimiter") or ","
    )

    raw_target_file_encoding = str(
        table_config.get("target_file_encoding") or "utf-8"
    )

    raw_target_file_dir = (
        table_config.get("target_file_dir") or ""
    ).strip()

    raw_target_pre_cmd = (
        table_config.get("target_pre_cmd") or ""
    ).strip()

    raw_target_post_cmd = (
        table_config.get("target_post_cmd") or ""
    ).strip()

    raw_input_param = table_config.get("input_param")
    config_option = table_config.get("config_option") or {}

    source_conn_name = (
        table_config.get("source_conn_name") or ""
    ).strip()

    print(
        f"START ETL "
        f"dag_id={dag_id}, "
        f"task_name={meta_task_name}, "
        f"airflow_task_id={runtime_info['task_id']}, "
        f"exec_seq={exec_seq}, "
        f"source_table={source_table}, "
        f"target_table={raw_target_file_name}"
    )

    run_hist_id = None
    extract_row_count = 0
    file_write_row_count = 0
    target_file_path = ""

    try:
        run_hist_id = insert_etl_run_hist(
            dag_id=dag_id,
            run_id=runtime_info["run_id"],
            task_id=meta_task_name,
            map_index=runtime_info.get("map_index", -1),
            source_table=source_table,
            target_table=raw_target_file_name,
            load_option="FILE",
            source_conn_name=source_conn_name,
            target_conn_name="",
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

        target_file_type = apply_input_params_for_file(
            raw_target_file_type,
            input_params,
        ).lower()

        csv_file_delimiter = apply_input_params_for_file(
            raw_csv_file_delimiter,
            input_params,
        )

        normalized_csv_file_delimiter = normalize_csv_delimiter(
            csv_file_delimiter
        )

        target_file_encoding = apply_input_params_for_file(
            raw_target_file_encoding,
            input_params,
        )

        normalized_target_file_encoding = normalize_file_encoding(
            target_file_encoding
        )

        target_file_dir = apply_input_params_for_file(
            raw_target_file_dir,
            input_params,
        )

        target_pre_cmd = apply_input_params_for_file(
            raw_target_pre_cmd,
            input_params,
        )

        target_post_cmd = apply_input_params_for_file(
            raw_target_post_cmd,
            input_params,
        )

        target_file_name = apply_input_params_for_file(
            raw_target_file_name,
            input_params,
        )

        if not source_exec_sql:
            raise ValueError(
                f"{target_file_name}: "
                f"source_exec_sql is empty after param replacement"
            )

        if target_file_type not in ("json", "csv", "text"):
            raise ValueError(
                f"{target_file_name}: invalid target_file_type "
                f"[{target_file_type}]"
            )

        if target_file_type == "csv" and not normalized_csv_file_delimiter:
            raise ValueError(
                f"{target_file_name}: "
                f"csv_file_delimiter is empty after param replacement"
            )

        full_target_file_path = str(Path(target_file_dir) / target_file_name)
        target_file_path = full_target_file_path

        print(f"[DEBUG] dag_id={dag_id}")
        print(f"[DEBUG] task_name={meta_task_name}")
        print(f"[DEBUG] airflow_task_id={runtime_info['task_id']}")
        print(f"[DEBUG] exec_seq={exec_seq}")
        print(f"[DEBUG] source_table={source_table}")
        print(f"[DEBUG] source_conn_name={source_conn_name}")
        print(f"[DEBUG] target_file_name={target_file_name}")
        print(f"[DEBUG] full_target_file_path={full_target_file_path}")
        print(f"[DEBUG] target_file_type={target_file_type}")

        source_hook = OdbcHook(odbc_conn_id=source_conn_name)

        source_conn = None
        meta_cursor = None
        source_cursor = None

        job_succeeded = False

        try:
            if target_pre_cmd:
                completed = subprocess.run(
                    target_pre_cmd,
                    shell=True,
                    check=True,
                    capture_output=True,
                    text=True,
                )

                if completed.stdout:
                    print(completed.stdout)

                if completed.stderr:
                    print(completed.stderr)

            source_conn = source_hook.get_conn()

            meta_cursor = source_conn.cursor()
            meta_sql = build_limit_0_sql_for_oracle(source_exec_sql)
            meta_cursor.execute(meta_sql)

            if meta_cursor.description is None:
                raise ValueError(
                    f"{target_file_name}: "
                    f"source_exec_sql did not return a result set. "
                    f"Only SELECT query is allowed. "
                    f"source_exec_sql=[{source_exec_sql}]"
                )

            source_columns = [desc[0] for desc in meta_cursor.description]

            if not source_columns:
                raise ValueError(
                    f"{target_file_name}: "
                    f"source_exec_sql returned no columns. "
                    f"source_exec_sql=[{source_exec_sql}]"
                )

            column_mapping = parse_column_mapping(raw_column_mapping) or {}

            target_columns = [
                column_mapping.get(src_col, src_col)
                for src_col in source_columns
            ]

            if len(set(target_columns)) != len(target_columns):
                raise ValueError(
                    f"{target_file_name}: "
                    f"duplicate target columns detected after mapping. "
                    f"source_columns={source_columns}, "
                    f"target_columns={target_columns}"
                )

            if target_file_type == "text" and len(target_columns) != 1:
                raise ValueError(
                    f"{target_file_name}: "
                    f"text target requires exactly one column. "
                    f"target_columns={target_columns}"
                )

            source_cursor = source_conn.cursor()
            source_cursor.execute(source_exec_sql)
            rows = source_cursor.fetchall()

            extract_row_count = len(rows)
            file_write_row_count = len(rows)

            print(f"[DEBUG] source_columns={source_columns}")
            print(f"[DEBUG] target_columns={target_columns}")
            print(f"[DEBUG] total_fetched_rows={len(rows)}")
            print(f"[DEBUG] sample_rows={rows[:5]}")

            if target_file_type == "csv":
                write_csv_file(
                    file_path=full_target_file_path,
                    columns=target_columns,
                    rows=rows,
                    delimiter=normalized_csv_file_delimiter,
                    encoding=normalized_target_file_encoding,
                )

            elif target_file_type == "json":
                write_json_file(
                    file_path=full_target_file_path,
                    columns=target_columns,
                    rows=rows,
                    encoding=normalized_target_file_encoding,
                )

            elif target_file_type == "text":
                write_text_file(
                    file_path=full_target_file_path,
                    columns=target_columns,
                    rows=rows,
                    encoding=normalized_target_file_encoding,
                )

            if target_post_cmd:
                completed = subprocess.run(
                    target_post_cmd,
                    shell=True,
                    check=True,
                    capture_output=True,
                    text=True,
                )

                if completed.stdout:
                    print(completed.stdout)

                if completed.stderr:
                    print(completed.stderr)

            job_succeeded = True

        finally:
            if meta_cursor is not None:
                meta_cursor.close()

            if source_cursor is not None:
                source_cursor.close()

            if source_conn is not None:
                source_conn.close()

            print(f"[DEBUG] job_succeeded={job_succeeded}")

        update_etl_run_hist_success(
            run_hist_id=run_hist_id,
            extract_row_count=extract_row_count,
            file_write_row_count=file_write_row_count,
            target_file_path=target_file_path,
        )

        print(
            f"END ETL SUCCESS "
            f"dag_id={dag_id}, "
            f"task_name={meta_task_name}, "
            f"airflow_task_id={runtime_info['task_id']}, "
            f"exec_seq={exec_seq}, "
            f"source_table={source_table}, "
            f"target_table={raw_target_file_name}"
        )

    except Exception:
        print(
            f"END ETL FAILED "
            f"dag_id={dag_id}, "
            f"task_name={meta_task_name}, "
            f"exec_seq={exec_seq}, "
            f"source_table={source_table}, "
            f"target_table={raw_target_file_name}"
        )

        if run_hist_id is not None:
            update_etl_run_hist_failed(
                run_hist_id=run_hist_id,
                error_message=traceback.format_exc(),
                extract_row_count=extract_row_count,
                file_write_row_count=file_write_row_count,
                target_file_path=target_file_path,
            )

        raise


def create_odbc_to_file_meta_task(
    dag_id: str,
    task_name: str,
    meta_postgres_conn_id: str = DEFAULT_META_POSTGRES_CONN_ID,
    airflow_task_id: str | None = None,
    pool_slots: int = 1,
):
    """
    static Airflow task 생성 함수.

    dynamic expand 사용 안 함.
    dag_id + task_name 기준으로 공통 ETL 코드 수행.
    """

    static_task_id = normalize_airflow_task_id(
        airflow_task_id or task_name
    )

    @task(task_id=static_task_id, pool_slots=pool_slots)
    def _run_static_etl(**context):
        run_odbc_to_file_common(
            dag_id=dag_id,
            task_name=task_name,
            meta_postgres_conn_id=meta_postgres_conn_id,
            **context,
        )

    return _run_static_etl()