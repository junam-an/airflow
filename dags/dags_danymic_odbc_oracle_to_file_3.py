from __future__ import annotations

import csv
import json
import subprocess
import traceback
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.etl_hist_utils import (
    get_task_runtime_info,
    insert_etl_run_hist,
    update_etl_run_hist_success,
    update_etl_run_hist_failed,
)


DAG_ID = "DYNAMIC_ODBC_ORACLE_TO_FILE_ETL_META_3"

META_POSTGRES_CONN_ID = "postgres_conn"


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
        raise ValueError(f"Invalid config_option JSON: {raw_config_option}") from e

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
    SQL 용 치환: 값 그대로 치환
    예) $$p_base_dt -> '20260314'
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


def apply_input_params_for_file(text: str | None, input_params: dict[str, str]) -> str:
    """
    파일명/디렉터리/OS command 용 치환:
    바깥 작은따옴표 제거 후 치환
    예) $$p_base_dt -> 20260314
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
    """
    허용값:
      - utf-8
      - euc-kr
      - cp949
    별칭:
      - utf8  -> utf-8
      - euckr -> euc-kr
      - ms949 -> cp949
    """
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


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
) as dag:

    @task
    def get_table_configs():
        meta_hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)

        insert_etl_param_sql = """
        INSERT INTO ETL_PARAM
        WITH BASE_PARAM AS
        (
        SELECT
        YESTERDAY_DT AS P_BASE_DT
        , YESTERDAY_DT AS P_START_DT
        , YESTERDAY_DT AS P_END_DT
        , BEF_MAX_DAY AS P_BEF_MAX_DT
        , MAX_DAY AS P_MAX_DT
        FROM ETL_CALENDAR
        WHERE 1=1
        AND TODAY_DT = TO_CHAR(TIMEZONE('ASIA/SEOUL', CURRENT_DATE)::TIMESTAMP, 'YYYYMMDD')
        )
        SELECT 
        DAG_ID
        , A.SOURCE_TABLE
        , A.TARGET_TABLE
        , '{"$$P_BASE_DT":"' || P_BASE_DT || 
        '","$$P_START_DT":"' || P_START_DT ||
        '","$$P_END_DT":"' || P_END_DT ||
        '","$$P_START_TM":"' || (INPUT_PARAM::JSON ->> '$$P_END_TM') ||
        '","$$P_END_TM":"' || TO_CHAR(TIMEZONE('ASIA/SEOUL', NOW())::TIMESTAMP, 'YYYYMMDDHH24MISS') ||
        '","$$P_BEF_MAX_DT":"' || P_BEF_MAX_DT ||
        '","$$P_MAX_DT":"' || P_MAX_DT ||
        '"}' AS TOBE_PARAM
        , A.INPUT_PARAM AS ASIS_PARAM
        , TIMEZONE('ASIA/SEOUL', NOW())::TIMESTAMP AS CREATED_TM
        , 'AIRFLOW' AS CREATE_USER_ID
        FROM ETL_META_DB_TO_FILE A, BASE_PARAM B
        WHERE 1=1
        AND DAG_ID = %s
        AND DISABLE_DT = '20991231'
        AND ENABLE_YN = 'Y'
        """

        update_input_param_sql = """
        UPDATE etl_meta_db_to_file a
        SET input_param = b.tobe_param
        FROM (
            SELECT dag_id, source_table, target_table, tobe_param
            FROM etl_param a
            WHERE dag_id = %s
              AND (source_table, target_table, created_tm) = (
                  SELECT b.source_table, b.target_table, b.created_tm
                  FROM (
                      SELECT dag_id, source_table, target_table, created_tm
                      FROM etl_param
                      WHERE dag_id = a.dag_id
                        AND source_table = a.source_table
                        AND target_table = a.target_table
                      ORDER BY created_tm DESC
                  ) b
                  LIMIT 1
              )
        ) b
        WHERE a.dag_id = b.dag_id
          AND a.source_table = b.source_table
          AND a.target_table = b.target_table
        """

        select_meta_sql = """
        SELECT
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
            input_param,
            config_option
        FROM etl_meta_db_to_file
        WHERE 1=1
          AND enable_yn = 'Y'
          AND dag_id = %s
          AND disable_dt = '20991231'
        """

        conn = None
        cursor = None

        try:
            conn = meta_hook.get_conn()
            conn.autocommit = False
            cursor = conn.cursor()

            cursor.execute("SET TIME ZONE 'Asia/Seoul'")

            cursor.execute(insert_etl_param_sql, (DAG_ID,))
            conn.commit()

            cursor.execute(update_input_param_sql, (DAG_ID,))
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
            source_exec_sql = (r[2] or "").strip() if r[2] is not None else ""
            column_mapping = r[3]
            target_file_type = (r[4] or "").strip().lower() if r[4] is not None else ""
            csv_file_delimiter = (r[5] or ",") if r[5] is not None else ","
            target_file_encoding = (r[6] or "utf-8").strip() if r[6] is not None else "utf-8"
            target_file_dir = (r[7] or "").strip() if r[7] is not None else ""
            target_pre_cmd = (r[8] or "").strip() if r[8] is not None else ""
            target_post_cmd = (r[9] or "").strip() if r[9] is not None else ""
            input_param = (r[10] or "").strip() if r[10] is not None else ""
            config_option = (r[11] or "").strip() if r[11] is not None else ""

            normalized_target_file_encoding = normalize_file_encoding(target_file_encoding)
            parsed_config_option = parse_config_option(config_option)
            source_conn_name = parsed_config_option.get("SOURCE_CONN_NAME", "").strip()

            if not source_table:
                raise ValueError("etl_meta.source_table is empty")

            if not target_table:
                raise ValueError("etl_meta.target_table(file_name) is empty")

            if not source_exec_sql:
                raise ValueError(
                    f"{target_table}: source_exec_sql is empty"
                )

            if target_file_type not in ("json", "csv", "text"):
                raise ValueError(
                    f"{target_table}: invalid target_file_type [{target_file_type}]. "
                    f"Allowed values are json, csv, text."
                )

            if target_file_type == "csv" and not csv_file_delimiter:
                raise ValueError(
                    f"{target_table}: csv_file_delimiter is empty for csv target"
                )

            if not target_file_dir:
                raise ValueError(
                    f"{target_table}: target_file_dir is empty"
                )

            if not source_conn_name:
                raise ValueError(
                    f"{target_table}: config_option.SOURCE_CONN_NAME is empty"
                )

            configs.append(
                {
                    "source_table": source_table,
                    "target_table": target_table,
                    "source_exec_sql": source_exec_sql,
                    "column_mapping": column_mapping,
                    "target_file_type": target_file_type,
                    "csv_file_delimiter": csv_file_delimiter,
                    "target_file_encoding": normalized_target_file_encoding,
                    "target_file_dir": target_file_dir,
                    "target_pre_cmd": target_pre_cmd,
                    "target_post_cmd": target_post_cmd,
                    "input_param": input_param,
                    "config_option": parsed_config_option,
                    "source_conn_name": source_conn_name,
                }
            )

        return configs

    @task(pool_slots=1)
    def run_etl(table_config: dict, **context):
        runtime_info = get_task_runtime_info(**context)

        source_table = (table_config.get("source_table") or "").strip()
        raw_target_file_name = (table_config.get("target_table") or "").strip()
        raw_source_exec_sql = (table_config.get("source_exec_sql") or "").strip()
        raw_column_mapping = table_config.get("column_mapping")
        raw_target_file_type = (table_config.get("target_file_type") or "").strip().lower()
        raw_csv_file_delimiter = str(table_config.get("csv_file_delimiter") or ",")
        raw_target_file_encoding = str(table_config.get("target_file_encoding") or "utf-8")
        raw_target_file_dir = (table_config.get("target_file_dir") or "").strip()
        raw_target_pre_cmd = (table_config.get("target_pre_cmd") or "").strip()
        raw_target_post_cmd = (table_config.get("target_post_cmd") or "").strip()
        raw_input_param = table_config.get("input_param")
        config_option = table_config.get("config_option") or {}
        source_conn_name = (table_config.get("source_conn_name") or "").strip()

        run_hist_id = None
        extract_row_count = 0
        file_write_row_count = 0
        target_file_path = ""

        if not raw_target_file_name:
            raise ValueError("table_config.target_table(file_name) is empty")

        if not raw_source_exec_sql:
            raise ValueError(f"{raw_target_file_name}: source_exec_sql is empty")

        if raw_target_file_type not in ("json", "csv", "text"):
            raise ValueError(
                f"{raw_target_file_name}: invalid target_file_type [{raw_target_file_type}]"
            )

        if not raw_target_file_dir:
            raise ValueError(f"{raw_target_file_name}: target_file_dir is empty")

        if not source_conn_name:
            raise ValueError(
                f"{raw_target_file_name}: source_conn_name is empty"
            )

        try:
            run_hist_id = insert_etl_run_hist(
                dag_id=DAG_ID,
                run_id=runtime_info["run_id"],
                task_id=runtime_info["task_id"],
                map_index=runtime_info["map_index"],
                source_table=source_table,
                target_table=raw_target_file_name,
                load_option="FILE",
                source_conn_name=source_conn_name,
                target_conn_name="",
                input_param=raw_input_param,
                config_option=config_option,
            )

            input_params = parse_input_params(raw_input_param)

            source_exec_sql = apply_input_params(raw_source_exec_sql, input_params)

            target_file_type = apply_input_params_for_file(raw_target_file_type, input_params).lower()
            csv_file_delimiter = apply_input_params_for_file(raw_csv_file_delimiter, input_params)
            normalized_csv_file_delimiter = normalize_csv_delimiter(csv_file_delimiter)

            target_file_encoding = apply_input_params_for_file(raw_target_file_encoding, input_params)
            normalized_target_file_encoding = normalize_file_encoding(target_file_encoding)

            target_file_dir = apply_input_params_for_file(raw_target_file_dir, input_params)
            target_pre_cmd = apply_input_params_for_file(raw_target_pre_cmd, input_params)
            target_post_cmd = apply_input_params_for_file(raw_target_post_cmd, input_params)
            target_file_name = apply_input_params_for_file(raw_target_file_name, input_params)

            if not source_exec_sql:
                raise ValueError(
                    f"{target_file_name}: source_exec_sql is empty after param replacement"
                )

            if target_file_type == "csv" and not normalized_csv_file_delimiter:
                raise ValueError(
                    f"{target_file_name}: csv_file_delimiter is empty after param replacement"
                )

            full_target_file_path = str(Path(target_file_dir) / target_file_name)
            target_file_path = full_target_file_path

            print(f"[DEBUG] source_table={source_table}")
            print(f"[DEBUG] source_conn_name={source_conn_name}")
            print(f"[DEBUG] target_file_name={target_file_name}")
            print(f"[DEBUG] target_file_dir={target_file_dir}")
            print(f"[DEBUG] full_target_file_path={full_target_file_path}")
            print(f"[DEBUG] target_file_type={target_file_type}")
            print(f"[DEBUG] csv_file_delimiter_raw={csv_file_delimiter}")
            print(f"[DEBUG] csv_file_delimiter_normalized={repr(normalized_csv_file_delimiter)}")
            print(f"[DEBUG] target_file_encoding_raw={target_file_encoding}")
            print(f"[DEBUG] target_file_encoding_normalized={normalized_target_file_encoding}")

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
                        f"{target_file_name}: source_exec_sql did not return a result set. "
                        f"Only SELECT query is allowed. source_exec_sql=[{source_exec_sql}]"
                    )

                source_columns = [desc[0] for desc in meta_cursor.description]

                if not source_columns:
                    raise ValueError(
                        f"{target_file_name}: source_exec_sql returned no columns. "
                        f"source_exec_sql=[{source_exec_sql}]"
                    )

                column_mapping = parse_column_mapping(raw_column_mapping) or {}

                target_columns = [
                    column_mapping.get(src_col, src_col)
                    for src_col in source_columns
                ]

                if not target_columns:
                    raise ValueError(
                        f"{target_file_name}: mapped target_columns is empty"
                    )

                if len(set(target_columns)) != len(target_columns):
                    raise ValueError(
                        f"{target_file_name}: duplicate target columns detected after mapping. "
                        f"source_columns={source_columns}, target_columns={target_columns}"
                    )

                if target_file_type == "text" and len(target_columns) != 1:
                    raise ValueError(
                        f"{target_file_name}: text target requires exactly one column. "
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

                print(
                    f"{source_table or '[source_sql]'} -> {full_target_file_path} "
                    f"rows={len(rows)} file_type={target_file_type} "
                    f"encoding={normalized_target_file_encoding}"
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

        except Exception:
            if run_hist_id is not None:
                update_etl_run_hist_failed(
                    run_hist_id=run_hist_id,
                    error_message=traceback.format_exc(),
                    extract_row_count=extract_row_count,
                    file_write_row_count=file_write_row_count,
                    target_file_path=target_file_path,
                )
            raise

    table_configs = get_table_configs()
    run_etl.expand(table_config=table_configs)