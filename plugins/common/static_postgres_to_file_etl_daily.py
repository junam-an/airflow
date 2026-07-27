from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import subprocess
import sys
import traceback
import uuid
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

try:
    from airflow.decorators import task as airflow_task
    from airflow.providers.postgres.hooks.postgres import (
        PostgresHook as AirflowPostgresHook,
    )
except ImportError:
    airflow_task = None
    AirflowPostgresHook = None

try:
    import psycopg2
except ImportError:
    psycopg2 = None


DEFAULT_META_POSTGRES_CONN_ID = "postgres_conn"
HIST_TABLE_NAME = "etl_job_run_dtl_hist"
USE_AIRFLOW_HOOKS = True


def build_airflow_conn_env_name(conn_id: str) -> str:
    normalized = "".join(
        ch if ch.isalnum() else "_"
        for ch in conn_id.upper()
    )
    return f"AIRFLOW_CONN_{normalized}"


def parse_postgres_conn_uri(conn_id: str, uri: str) -> dict:
    parsed = urlparse(uri)
    if parsed.scheme not in ("postgres", "postgresql"):
        raise ValueError(
            f"{conn_id}: unsupported connection URI scheme "
            f"[{parsed.scheme}]. Use postgres:// or postgresql://."
        )

    query = parse_qs(parsed.query)
    conn_kwargs = {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "dbname": unquote(parsed.path.lstrip("/")),
        "user": unquote(parsed.username or ""),
        "password": unquote(parsed.password or ""),
    }

    for key, values in query.items():
        if values:
            conn_kwargs[key] = values[-1]

    return {k: v for k, v in conn_kwargs.items() if v not in (None, "")}


def get_env_value(conn_id: str, suffix: str, default: str = "") -> str:
    prefix = "".join(ch if ch.isalnum() else "_" for ch in conn_id.upper())
    return os.getenv(f"{prefix}_{suffix}", default)


def build_postgres_conn_kwargs_from_env(conn_id: str) -> dict:
    airflow_conn_uri = os.getenv(build_airflow_conn_env_name(conn_id))
    if airflow_conn_uri:
        return parse_postgres_conn_uri(conn_id, airflow_conn_uri)

    kwargs = {
        "host": get_env_value(conn_id, "HOST"),
        "port": get_env_value(conn_id, "PORT", "5432"),
        "dbname": (
            get_env_value(conn_id, "DBNAME")
            or get_env_value(conn_id, "DATABASE")
        ),
        "user": get_env_value(conn_id, "USER"),
        "password": get_env_value(conn_id, "PASSWORD"),
    }

    extra_raw = get_env_value(conn_id, "EXTRA")
    if extra_raw:
        extra = json.loads(extra_raw)
        if not isinstance(extra, dict):
            raise ValueError(f"{conn_id}_EXTRA must be JSON object")
        kwargs.update(extra)

    missing = [key for key in ("host", "dbname", "user") if not kwargs.get(key)]
    if missing:
        env_name = build_airflow_conn_env_name(conn_id)
        raise ValueError(
            f"Postgres connection [{conn_id}] is not configured. "
            f"Set {env_name}=postgresql://user:password@host:5432/dbname "
            f"or set {conn_id.upper()}_HOST, _DBNAME, _USER, _PASSWORD. "
            f"Missing: {missing}"
        )

    return {k: v for k, v in kwargs.items() if v not in (None, "")}


class StandalonePostgresHook:
    def __init__(self, postgres_conn_id: str):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        if psycopg2 is None:
            raise ImportError(
                "psycopg2 is required for standalone Python execution. "
                "Install it with: pip install psycopg2-binary"
            )
        return psycopg2.connect(
            **build_postgres_conn_kwargs_from_env(self.postgres_conn_id)
        )

    def run(self, sql: str, parameters=None) -> None:
        conn = None
        cursor = None
        try:
            conn = self.get_conn()
            conn.autocommit = False
            cursor = conn.cursor()
            cursor.execute(sql, parameters)
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


def get_postgres_hook(postgres_conn_id: str):
    if USE_AIRFLOW_HOOKS and AirflowPostgresHook is not None:
        return AirflowPostgresHook(postgres_conn_id=postgres_conn_id)
    return StandalonePostgresHook(postgres_conn_id=postgres_conn_id)


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
    task_id = context.get("task_id") or getattr(task, "task_id", "standalone_task")
    run_id = context.get("run_id") or f"manual__{datetime.now():%Y%m%dT%H%M%S}__{uuid.uuid4().hex[:8]}"

    return {
        "run_id": run_id,
        "task_id": task_id,
        "map_index": context.get("map_index", getattr(ti, "map_index", -1)),
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
    create_user_id: str = "python",
    meta_conn_id: str = DEFAULT_META_POSTGRES_CONN_ID,
) -> int:
    hook = get_postgres_hook(meta_conn_id)
    insert_sql = f"""
        INSERT INTO {HIST_TABLE_NAME} (
            dag_id, run_id, task_id, map_index, source_table, target_table,
            load_option, source_conn_name, target_conn_name,
            extract_row_count, stg_load_row_count, target_insert_count,
            target_update_count, target_delete_count,
            target_total_affected_count, file_write_row_count,
            target_file_path, status, error_message, start_tm, end_tm,
            input_param, config_option, create_user_id, create_tm, update_tm
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
                dag_id, run_id, task_id, map_index, source_table or "",
                target_table or "", load_option or "", source_conn_name or "",
                target_conn_name or "", safe_json_dumps(input_param),
                safe_json_dumps(config_option), create_user_id,
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
    meta_conn_id: str = DEFAULT_META_POSTGRES_CONN_ID,
) -> None:
    hook = get_postgres_hook(meta_conn_id)
    total_affected = int(target_insert_count) + int(target_update_count) + int(target_delete_count)
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
    hook.run(update_sql, parameters=(
        extract_row_count, stg_load_row_count, target_insert_count,
        target_update_count, target_delete_count, total_affected,
        file_write_row_count, target_file_path or "", run_hist_id,
    ))


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
    meta_conn_id: str = DEFAULT_META_POSTGRES_CONN_ID,
) -> None:
    hook = get_postgres_hook(meta_conn_id)
    total_affected = int(target_insert_count) + int(target_update_count) + int(target_delete_count)
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
    hook.run(update_sql, parameters=(
        extract_row_count, stg_load_row_count, target_insert_count,
        target_update_count, target_delete_count, total_affected,
        file_write_row_count, target_file_path or "", cut_text(error_message, 4000),
        run_hist_id,
    ))


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


def apply_input_params(text: str | None, input_params: dict[str, str]) -> str:
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


def parse_encryption_columns(raw_columns: str | None) -> list[str]:
    if raw_columns is None:
        return []

    raw_columns = str(raw_columns).strip()
    if not raw_columns:
        return []

    return [
        col.strip()
        for col in raw_columns.split(",")
        if col.strip()
    ]


def simple_encrypt_value(value) -> str:
    if value is None:
        return ""

    text = str(value)
    if not text:
        return ""

    key = b"temporary-test-key"
    data = text.encode("utf-8")
    encrypted = bytes(
        byte ^ key[idx % len(key)]
        for idx, byte in enumerate(data)
    )
    return "ENC$" + base64.urlsafe_b64encode(encrypted).decode("ascii")


def find_column_index(columns: list[str], column_name: str) -> int | None:
    normalized_column_name = column_name.strip().lower()
    for idx, column in enumerate(columns):
        if str(column).strip().lower() == normalized_column_name:
            return idx
    return None


def resolve_encryption_column_indexes(
    source_columns: list[str],
    target_columns: list[str],
    encryption_columns: list[str],
) -> list[int]:
    indexes = []
    missing_columns = []

    for column_name in encryption_columns:
        idx = find_column_index(target_columns, column_name)
        if idx is None:
            idx = find_column_index(source_columns, column_name)

        if idx is None:
            missing_columns.append(column_name)
            continue

        if idx not in indexes:
            indexes.append(idx)

    if missing_columns:
        raise ValueError(
            f"ENCRYPTION_COL contains unknown columns. "
            f"missing={missing_columns}, "
            f"source_columns={source_columns}, "
            f"target_columns={target_columns}"
        )

    return indexes


def encrypt_row_values(
    rows: list[tuple],
    encryption_column_indexes: list[int],
) -> list[tuple]:
    if not encryption_column_indexes:
        return rows

    encrypted_rows = []
    for row in rows:
        values = list(row)
        for idx in encryption_column_indexes:
            values[idx] = simple_encrypt_value(values[idx])
        encrypted_rows.append(tuple(values))

    return encrypted_rows

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


def build_limit_0_sql(source_exec_sql: str) -> str:
    return f"""
        SELECT *
        FROM (
            {source_exec_sql}
        ) q
        LIMIT 0
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
        json.dump(data, f, ensure_ascii=False, indent=2)


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


def get_single_table_config(
    dag_id: str,
    task_name: str,
    meta_postgres_conn_id: str = DEFAULT_META_POSTGRES_CONN_ID,
    today_dt: str | None = None,
) -> dict:
    """
    static task에서 직접 호출되는 실제 PostgreSQL -> File ETL 공통 함수.
    dag_id + task_name 기준으로 메타 1건 조회 후 ETL 수행.
    """
    meta_hook = get_postgres_hook(meta_postgres_conn_id)

    insert_etl_param_sql = """
    INSERT INTO ETL_PARAM
    WITH BASE_PARAM AS
    (
    SELECT
    YESTERDAY_DT AS P_BASE_DT
    , yyyy || mm || '01' AS P_START_DT
    , YESTERDAY_DT AS P_END_DT
    , BEF_MAX_DAY AS P_BEF_MAX_DT
    , MAX_DAY AS P_MAX_DT
    , yyyy || mm as P_BASE_YM
    FROM ETL_CALENDAR
    WHERE 1=1
    AND TODAY_DT = %s
    )
    SELECT
    DAG_ID
    , TASK_NAME
    , '{"$$P_BASE_DT":"' || P_BASE_DT ||
    '","$$P_START_DT":"' || P_START_DT ||
    '","$$P_END_DT":"' || P_END_DT ||
    '","$$P_START_TM":"' || (INPUT_PARAM::JSON ->> '$$P_END_TM') ||
    '","$$P_END_TM":"' || TO_CHAR(TIMEZONE('ASIA/SEOUL', NOW())::TIMESTAMP, 'YYYYMMDDHH24MISS') ||
    '","$$P_BEF_MAX_DT":"' || P_BEF_MAX_DT ||
    '","$$P_MAX_DT":"' || P_MAX_DT ||
    '","$$P_BASE_YM":"' || P_BASE_YM ||
    '"}' AS TOBE_PARAM
    , A.INPUT_PARAM AS ASIS_PARAM
    , TIMEZONE('ASIA/SEOUL', NOW())::TIMESTAMP AS CREATED_TM
    , 'AIRFLOW' AS CREATE_USER_ID
    FROM etl_meta_db_to_file A, BASE_PARAM B
    WHERE 1=1
    AND DAG_ID = %s
    AND TASK_NAME = %s
    AND DISABLE_DT = '20991231'
    AND ENABLE_YN = 'Y'
    """

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

        if today_dt is not None:
            cursor.execute(insert_etl_param_sql, (today_dt, dag_id, task_name))

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
            f"ETL meta must be unique. dag_id={dag_id}, "
            f"task_name={task_name}, count={len(rows)}"
        )

    r = rows[0]

    meta_task_name = (r[0] or "").strip() if r[0] is not None else ""
    exec_seq = int(r[1]) if r[1] is not None else 999999

    source_table = (r[2] or "").strip() if r[2] is not None else ""
    target_table = (r[3] or "").strip() if r[3] is not None else ""
    source_exec_sql = (r[4] or "").strip() if r[4] is not None else ""
    column_mapping = r[5]
    target_file_type = (r[6] or "").strip().lower() if r[6] is not None else ""
    csv_file_delimiter = (r[7] or ",") if r[7] is not None else ","
    target_file_encoding = (
        (r[8] or "utf-8").strip()
        if r[8] is not None
        else "utf-8"
    )
    target_file_dir = (r[9] or "").strip() if r[9] is not None else ""
    target_pre_cmd = (r[10] or "").strip() if r[10] is not None else ""
    target_post_cmd = (r[11] or "").strip() if r[11] is not None else ""
    config_option = (r[12] or "").strip() if r[12] is not None else ""
    input_param = (r[13] or "").strip() if r[13] is not None else ""

    if not meta_task_name:
        meta_task_name = task_name

    normalized_target_file_encoding = normalize_file_encoding(
        target_file_encoding
    )

    parsed_config_option = parse_config_option(config_option)
    source_conn_name = (
        parsed_config_option.get("SOURCE_CONN_NAME") or ""
    ).strip()
    encryption_columns = parse_encryption_columns(
        parsed_config_option.get("ENCRYPTION_COL")
    )

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

    return {
        "task_name": meta_task_name,
        "exec_seq": exec_seq,
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
        "config_option": parsed_config_option,
        "input_param": input_param,
        "source_conn_name": source_conn_name,
        "encryption_columns": encryption_columns,
    }


def run_postgres_to_file_etl(
    dag_id: str,
    task_name: str,
    meta_postgres_conn_id: str = DEFAULT_META_POSTGRES_CONN_ID,
    today_dt: str | None = None,
    **context,
):
    """
    dag_id + task_name 기준으로 PostgreSQL -> File 메타 1건만 조회한다.
    dynamic mapping용 list가 아니라 static task용 dict 1건을 반환한다.
    """
    runtime_info = get_task_runtime_info(**context)

    table_config = get_single_table_config(
        dag_id=dag_id,
        task_name=task_name,
        meta_postgres_conn_id=meta_postgres_conn_id,
        today_dt=today_dt,
    )

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
    config_option = table_config.get("config_option") or {}
    raw_input_param = table_config.get("input_param")
    source_conn_name = (
        table_config.get("source_conn_name") or ""
    ).strip()
    encryption_columns = table_config.get("encryption_columns") or []

    print(
        f"START ETL "
        f"dag_id={dag_id}, "
        f"task_name={meta_task_name}, "
        f"exec_seq={exec_seq}, "
        f"source_table={source_table}, "
        f"target_file={raw_target_file_name}"
    )

    run_hist_id = None
    extract_row_count = 0
    file_write_row_count = 0
    target_file_path = ""

    try:
        run_hist_id = insert_etl_run_hist(
            dag_id=dag_id,
            run_id=runtime_info["run_id"],
            task_id=meta_task_name or runtime_info["task_id"],
            map_index=runtime_info["map_index"],
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
            meta_conn_id=meta_postgres_conn_id,
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

        if target_file_type == "csv" and not normalized_csv_file_delimiter:
            raise ValueError(
                f"{target_file_name}: "
                f"csv_file_delimiter is empty after param replacement"
            )

        full_target_file_path = str(Path(target_file_dir) / target_file_name)
        target_file_path = full_target_file_path

        print(f"[DEBUG] dag_id={dag_id}")
        print(f"[DEBUG] airflow_task_id={runtime_info['task_id']}")
        print(f"[DEBUG] meta_task_name={meta_task_name}")
        print(f"[DEBUG] exec_seq={exec_seq}")
        print(f"[DEBUG] source_table={source_table}")
        print(f"[DEBUG] source_conn_name={source_conn_name}")
        print(f"[DEBUG] target_file_name={target_file_name}")
        print(f"[DEBUG] target_file_dir={target_file_dir}")
        print(f"[DEBUG] full_target_file_path={full_target_file_path}")
        print(f"[DEBUG] target_file_type={target_file_type}")
        print(f"[DEBUG] encryption_columns={encryption_columns}")
        print(f"[DEBUG] csv_file_delimiter_raw={csv_file_delimiter}")
        print(
            f"[DEBUG] csv_file_delimiter_normalized="
            f"{repr(normalized_csv_file_delimiter)}"
        )
        print(f"[DEBUG] target_file_encoding_raw={target_file_encoding}")
        print(
            f"[DEBUG] target_file_encoding_normalized="
            f"{normalized_target_file_encoding}"
        )

        source_hook = get_postgres_hook(source_conn_name)

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
            meta_sql = build_limit_0_sql(source_exec_sql)
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

            if not target_columns:
                raise ValueError(
                    f"{target_file_name}: mapped target_columns is empty"
                )

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
            encryption_column_indexes = []
            rows_to_write = rows
            if target_file_type == "csv" and encryption_columns:
                encryption_column_indexes = resolve_encryption_column_indexes(
                    source_columns=source_columns,
                    target_columns=target_columns,
                    encryption_columns=encryption_columns,
                )
                rows_to_write = encrypt_row_values(
                    rows=rows_to_write,
                    encryption_column_indexes=encryption_column_indexes,
                )
            elif target_file_type != "csv" and encryption_columns:
                print(
                    f"[WARN] ENCRYPTION_COL is ignored for "
                    f"target_file_type={target_file_type}. "
                    f"CSV only is supported. "
                    f"encryption_columns={encryption_columns}"
                )
            print(f"[DEBUG] encryption_column_indexes={encryption_column_indexes}")
            print(f"[DEBUG] total_fetched_rows={len(rows)}")
            print(f"[DEBUG] sample_rows={rows[:5]}")

            if target_file_type == "csv":
                write_csv_file(
                    file_path=full_target_file_path,
                    columns=target_columns,
                    rows=rows_to_write,
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
                f"task_name={meta_task_name}, "
                f"exec_seq={exec_seq}, "
                f"{source_table or '[source_sql]'} "
                f"-> {full_target_file_path} "
                f"rows={len(rows)} "
                f"file_type={target_file_type} "
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
            meta_conn_id=meta_postgres_conn_id,
        )

        print(
            f"END ETL SUCCESS "
            f"dag_id={dag_id}, "
            f"task_name={meta_task_name}, "
            f"exec_seq={exec_seq}, "
            f"source_table={source_table}, "
            f"target_file_path={target_file_path}"
        )

    except Exception:
        print(
            f"END ETL FAILED "
            f"dag_id={dag_id}, "
            f"task_name={task_name}, "
            f"source_table={source_table}, "
            f"target_file={raw_target_file_name}"
        )

        if run_hist_id is not None:
            update_etl_run_hist_failed(
                run_hist_id=run_hist_id,
                error_message=traceback.format_exc(),
                extract_row_count=extract_row_count,
                file_write_row_count=file_write_row_count,
                target_file_path=target_file_path,
                meta_conn_id=meta_postgres_conn_id,
            )

        raise


def create_postgres_to_file_task(
    dag_id: str,
    task_name: str,
    task_id: str | None = None,
    meta_postgres_conn_id: str = DEFAULT_META_POSTGRES_CONN_ID,
    today_dt: str | None = None,
):
    """
    static Airflow task ?앹꽦 ?⑥닔.

    task_name:
        etl_meta_db_to_file.task_name 媛?

    task_id:
        Airflow UI???쒖떆??task_id.
        ?앸왂?섎㈃ task_name??洹몃?濡??ъ슜?쒕떎.
    """
    if airflow_task is None:
        raise RuntimeError(
            "create_postgres_to_file_task() requires Airflow. "
            "Use this file as a CLI script for standalone Python execution."
        )

    airflow_task_id = task_id or task_name

    @airflow_task(task_id=airflow_task_id)
    def _static_etl_task(**context):
        run_postgres_to_file_etl(
            dag_id=dag_id,
            task_name=task_name,
            meta_postgres_conn_id=meta_postgres_conn_id,
            today_dt=today_dt,
            **context,
        )

    return _static_etl_task()

def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run static PostgreSQL to file ETL without Airflow."
    )
    parser.add_argument("--dag-id", required=True, help="etl_meta_db_to_file.dag_id")
    parser.add_argument("--task-name", required=True, help="etl_meta_db_to_file.task_name")
    parser.add_argument("--today-dt", required=True, help="ETL_CALENDAR.TODAY_DT")
    parser.add_argument(
        "--meta-postgres-conn-id",
        default=DEFAULT_META_POSTGRES_CONN_ID,
        help="Metadata DB connection id. Default: postgres_conn",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run id for etl_job_run_dtl_hist.",
    )
    parser.add_argument(
        "--task-id",
        default=None,
        help="Optional task id for etl_job_run_dtl_hist.",
    )
    parser.add_argument(
        "--map-index",
        type=int,
        default=-1,
        help="Optional map index for etl_job_run_dtl_hist.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    global USE_AIRFLOW_HOOKS
    USE_AIRFLOW_HOOKS = False
    args = build_arg_parser().parse_args(argv)
    run_postgres_to_file_etl(
        dag_id=args.dag_id,
        task_name=args.task_name,
        meta_postgres_conn_id=args.meta_postgres_conn_id,
        today_dt=args.today_dt,
        run_id=args.run_id,
        task_id=args.task_id or args.task_name,
        map_index=args.map_index,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


