from __future__ import annotations

import argparse
import json
import os
import traceback
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

try:
    import psycopg2
except ImportError:
    psycopg2 = None


DEFAULT_META_FILE = "etl_meta_db_to_db.csv"
DEFAULT_PARAM_FILE = "etl_param.json"
DEFAULT_CALENDAR_FILE = "etl_calendar.csv"
DEFAULT_LOG_FILE = "logs/etl_run_log.jsonl"
DEFAULT_SOURCE_CONN_ID = "postgres_conn"
DEFAULT_TARGET_CONN_ID = "postgres_conn"
CHUNK_SIZE = 5000


# ****** standalone 전용 ******
def build_conn_env_name(conn_id: str) -> str:
    normalized = "".join(ch if ch.isalnum() else "_" for ch in conn_id.upper())
    return f"AIRFLOW_CONN_{normalized}"


# ****** standalone 전용 ******
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


# ****** standalone 전용 ******
def get_env_value(conn_id: str, suffix: str, default: str = "") -> str:
    prefix = "".join(ch if ch.isalnum() else "_" for ch in conn_id.upper())
    return os.getenv(f"{prefix}_{suffix}", default)


# ****** standalone 전용 ******
def build_postgres_conn_kwargs_from_env(conn_id: str) -> dict:
    conn_uri = os.getenv(build_conn_env_name(conn_id))
    if conn_uri:
        return parse_postgres_conn_uri(conn_id, conn_uri)
    kwargs = {
        "host": get_env_value(conn_id, "HOST"),
        "port": get_env_value(conn_id, "PORT", "5432"),
        "dbname": get_env_value(conn_id, "DBNAME") or get_env_value(conn_id, "DATABASE"),
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
        env_name = build_conn_env_name(conn_id)
        raise ValueError(
            f"Postgres connection [{conn_id}] is not configured. "
            f"Set {env_name}=postgresql://user:password@host:5432/dbname "
            f"or set {conn_id.upper()}_HOST, _DBNAME, _USER, _PASSWORD. "
            f"Missing: {missing}"
        )
    return {k: v for k, v in kwargs.items() if v not in (None, "")}


# ****** standalone 전용 ******
class StandalonePostgresHook:
    def __init__(self, postgres_conn_id: str):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        if psycopg2 is None:
            raise ImportError(
                "psycopg2 is required for standalone Python execution. "
                "Install it with: pip install psycopg2-binary"
            )
        return psycopg2.connect(**build_postgres_conn_kwargs_from_env(self.postgres_conn_id))

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

    def insert_rows(self, table: str, rows: list[tuple], target_fields: list[str] | None = None, commit_every: int = 1000, executemany: bool = False) -> None:
        if not rows:
            return
        fields_sql = ""
        if target_fields:
            fields_sql = " (" + ", ".join(target_fields) + ")"
        placeholders = ", ".join(["%s"] * len(rows[0]))
        insert_sql = f"INSERT INTO {table}{fields_sql} VALUES ({placeholders})"
        conn = None
        cursor = None
        try:
            conn = self.get_conn()
            conn.autocommit = False
            cursor = conn.cursor()
            if executemany:
                cursor.executemany(insert_sql, rows)
            else:
                for row in rows:
                    cursor.execute(insert_sql, row)
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


# ****** standalone 전용 ******
def get_postgres_hook(postgres_conn_id: str):
    return StandalonePostgresHook(postgres_conn_id=postgres_conn_id)


# ****** standalone 전용 ******
def safe_json_dumps(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, default=str)


# ****** standalone 전용 ******
def cut_text(value: str | None, max_length: int = 4000) -> str:
    if value is None:
        return ""
    text = str(value)
    if len(text) <= max_length:
        return text
    return text[:max_length]


# ****** standalone 전용 ******
def parse_csv_columns(raw_value: str | None) -> list[str]:
    if raw_value is None:
        return []

    return [c.strip() for c in raw_value.split(",") if c and c.strip()]


# ****** standalone 전용 ******
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


# ****** standalone 전용 ******
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


# ****** standalone 전용 ******
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


# ****** standalone 전용 ******
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


# ****** standalone 전용 ******
def build_limit_0_sql(source_exec_sql: str) -> str:
    return f"""
        SELECT *
        FROM (
            {source_exec_sql}
        ) q
        LIMIT 0
    """


# ****** standalone 전용 ******
def read_csv_dicts(file_path: str, encoding: str = "utf-8-sig") -> list[dict[str, str]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    with path.open("r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"CSV file has no header: {file_path}")
        return [
            {str(k).strip().lower(): "" if v is None else str(v).strip() for k, v in row.items()}
            for row in reader
        ]


# ****** standalone 전용 ******
def load_json_file(file_path: str) -> dict:
    path = Path(file_path)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"JSON file must contain object: {file_path}")
    return data


# ****** standalone 전용 ******
def write_json_file_atomic(file_path: str, data: dict) -> None:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
    temp_path.replace(path)


# ****** standalone 전용 ******
def normalize_param_key(dag_id: str, task_name: str) -> str:
    return f"{dag_id}::{task_name}"


# ****** standalone 전용 ******
def load_previous_input_param(param_file: str, dag_id: str, task_name: str) -> dict[str, str]:
    data = load_json_file(param_file)
    raw_value = data.get(normalize_param_key(dag_id, task_name), {})
    if isinstance(raw_value, dict) and "input_param" in raw_value:
        raw_value = raw_value.get("input_param") or {}
    if not isinstance(raw_value, dict):
        return {}
    return {str(k): "" if v is None else str(v) for k, v in raw_value.items()}


# ****** standalone 전용 ******
def save_current_input_param(param_file: str, dag_id: str, task_name: str, input_param: dict[str, str]) -> None:
    data = load_json_file(param_file)
    data[normalize_param_key(dag_id, task_name)] = {
        "dag_id": dag_id,
        "task_name": task_name,
        "input_param": input_param,
        "updated_tm": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    write_json_file_atomic(param_file, data)


# ****** standalone 전용 ******
def find_calendar_row(calendar_file: str, today_dt: str) -> dict[str, str] | None:
    if not Path(calendar_file).exists():
        return None
    for row in read_csv_dicts(calendar_file):
        if row.get("today_dt") == today_dt:
            return row
    return None


# ****** standalone 전용 ******
def build_calendar_values(today_dt: str, calendar_file: str) -> dict[str, str]:
    calendar_row = find_calendar_row(calendar_file, today_dt)
    if calendar_row is not None:
        yyyy = calendar_row.get("yyyy", "")
        mm = calendar_row.get("mm", "")
        return {
            "P_BASE_DT": calendar_row.get("yesterday_dt", ""),
            "P_START_DT": f"{yyyy}{mm}01" if yyyy and mm else "",
            "P_END_DT": calendar_row.get("yesterday_dt", ""),
            "P_BEF_MAX_DT": calendar_row.get("bef_max_day", ""),
            "P_MAX_DT": calendar_row.get("max_day", ""),
            "P_BASE_YM": f"{yyyy}{mm}" if yyyy and mm else "",
        }
    today = datetime.strptime(today_dt, "%Y%m%d")
    yesterday = today - timedelta(days=1)
    return {
        "P_BASE_DT": yesterday.strftime("%Y%m%d"),
        "P_START_DT": yesterday.strftime("%Y%m") + "01",
        "P_END_DT": yesterday.strftime("%Y%m%d"),
        "P_BEF_MAX_DT": "",
        "P_MAX_DT": "",
        "P_BASE_YM": yesterday.strftime("%Y%m"),
    }


# ****** standalone 전용 ******
def build_input_param_from_local_files(dag_id: str, task_name: str, today_dt: str | None, param_file: str, calendar_file: str, fallback_input_param: str | None) -> dict[str, str]:
    if not today_dt:
        previous = load_previous_input_param(param_file, dag_id, task_name)
        if previous:
            return previous
        return parse_input_params(fallback_input_param)

    previous = load_previous_input_param(param_file, dag_id, task_name)
    fallback = parse_input_params(fallback_input_param)
    calendar_values = build_calendar_values(today_dt, calendar_file)
    end_tm = datetime.now().strftime("%Y%m%d%H%M%S")
    start_tm = strip_outer_single_quotes(previous.get("$$P_END_TM") or fallback.get("$$P_END_TM") or "")
    input_param = {
        "$$P_BASE_DT": calendar_values.get("P_BASE_DT", ""),
        "$$P_START_DT": calendar_values.get("P_START_DT", ""),
        "$$P_END_DT": calendar_values.get("P_END_DT", ""),
        "$$P_START_TM": start_tm,
        "$$P_END_TM": end_tm,
        "$$P_BEF_MAX_DT": calendar_values.get("P_BEF_MAX_DT", ""),
        "$$P_MAX_DT": calendar_values.get("P_MAX_DT", ""),
        "$$P_BASE_YM": calendar_values.get("P_BASE_YM", ""),
    }
    save_current_input_param(param_file, dag_id, task_name, input_param)
    return input_param


# ****** standalone 전용 ******
def append_run_log(log_file: str, event: dict) -> None:
    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"event_tm": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), **event}
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False, default=str))
        f.write("\n")


# ****** standalone 전용 ******
def start_run_log(log_file: str, dag_id: str, run_id: str, task_id: str, task_name: str, exec_seq: int, source_table: str, target_table: str, load_option: str, source_conn_name: str, target_conn_name: str, input_param, config_option) -> None:
    append_run_log(log_file, {
        "status": "RUNNING", "dag_id": dag_id, "run_id": run_id,
        "task_id": task_id, "task_name": task_name, "exec_seq": exec_seq,
        "source_table": source_table, "target_table": target_table,
        "load_option": load_option, "source_conn_name": source_conn_name,
        "target_conn_name": target_conn_name, "input_param": input_param,
        "config_option": config_option,
    })


# ****** standalone 전용 ******
def finish_run_log(log_file: str, status: str, dag_id: str, run_id: str, task_id: str, task_name: str, exec_seq: int | None, source_table: str, target_table: str, extract_row_count: int, stg_load_row_count: int, target_insert_count: int, target_update_count: int, target_delete_count: int, file_write_row_count: int, error_message: str = "") -> None:
    append_run_log(log_file, {
        "status": status, "dag_id": dag_id, "run_id": run_id,
        "task_id": task_id, "task_name": task_name, "exec_seq": exec_seq,
        "source_table": source_table, "target_table": target_table,
        "extract_row_count": extract_row_count,
        "stg_load_row_count": stg_load_row_count,
        "target_insert_count": target_insert_count,
        "target_update_count": target_update_count,
        "target_delete_count": target_delete_count,
        "file_write_row_count": file_write_row_count,
        "error_message": cut_text(error_message, 4000),
    })

# ****** standalone 전용 ******
def get_csv_value(row: dict[str, str], key: str, default: str = "") -> str:
    return row.get(key.lower(), default)


# ****** standalone 전용 ******
def get_single_table_config(dag_id: str, task_name: str, meta_file: str, param_file: str, calendar_file: str, today_dt: str | None = None) -> dict:
    rows = read_csv_dicts(meta_file, encoding="utf-8-sig")
    matched_rows = [
        row for row in rows
        if get_csv_value(row, "dag_id") == dag_id
        and get_csv_value(row, "task_name") == task_name
        and get_csv_value(row, "enable_yn", "Y").upper() == "Y"
        and get_csv_value(row, "disable_dt", "20991231") == "20991231"
    ]
    if not matched_rows:
        raise ValueError(f"ETL meta not found. dag_id={dag_id}, task_name={task_name}, meta_file={meta_file}")
    if len(matched_rows) > 1:
        raise ValueError(f"ETL meta must be unique. dag_id={dag_id}, task_name={task_name}, count={len(matched_rows)}")

    r = matched_rows[0]
    meta_task_name = get_csv_value(r, "task_name") or task_name
    exec_seq_raw = get_csv_value(r, "exec_seq", "999999")
    exec_seq = int(exec_seq_raw) if exec_seq_raw else 999999
    source_table = get_csv_value(r, "source_table")
    target_table = get_csv_value(r, "target_table")
    pk_columns = parse_csv_columns(get_csv_value(r, "pk_column"))
    source_exec_sql = get_csv_value(r, "source_exec_sql")
    column_mapping = get_csv_value(r, "column_mapping")
    load_option = (get_csv_value(r, "load_option", "di") or "di").lower()
    stg_drop_yn = (get_csv_value(r, "stg_drop_yn", "N") or "N").upper()
    target_pre_sql = get_csv_value(r, "target_pre_sql")
    target_post_sql = get_csv_value(r, "target_post_sql")
    config_option = get_csv_value(r, "config_option")
    csv_input_param = get_csv_value(r, "input_param")

    parsed_config_option = parse_config_option(config_option)
    source_conn_name = (parsed_config_option.get("SOURCE_CONN_NAME") or parsed_config_option.get("source_conn_name") or DEFAULT_SOURCE_CONN_ID).strip()
    target_conn_name = (parsed_config_option.get("TARGET_CONN_NAME") or parsed_config_option.get("target_conn_name") or DEFAULT_TARGET_CONN_ID).strip()
    input_param = build_input_param_from_local_files(dag_id, task_name, today_dt, param_file, calendar_file, csv_input_param)

    if not target_table:
        raise ValueError("etl_meta.target_table is empty")
    if not source_exec_sql:
        raise ValueError(f"{target_table}: source_exec_sql is empty")
    if load_option not in ("ui", "di", "ti", "i", "u", "d"):
        raise ValueError(f"{target_table}: invalid load_option [{load_option}]")
    if stg_drop_yn not in ("Y", "N"):
        raise ValueError(f"{target_table}: invalid stg_drop_yn [{stg_drop_yn}]")
    if load_option in ("ui", "di", "u", "d") and not pk_columns:
        raise ValueError(f"{target_table}: pk_column is required for load_option [{load_option}]")
    if not source_conn_name:
        raise ValueError(f"{target_table}: config_option.SOURCE_CONN_NAME is empty")
    if not target_conn_name:
        raise ValueError(f"{target_table}: config_option.TARGET_CONN_NAME is empty")

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

# ****** standalone 전용 ******
def run_postgres_to_postgres_etl(
    dag_id: str,
    task_name: str,
    meta_file: str = DEFAULT_META_FILE,
    param_file: str = DEFAULT_PARAM_FILE,
    calendar_file: str = DEFAULT_CALENDAR_FILE,
    log_file: str = DEFAULT_LOG_FILE,
    today_dt: str | None = None,
    chunk_size: int = CHUNK_SIZE,
    run_id: str | None = None,
    task_id: str | None = None,
):
    """
    static task에서 직접 호출되는 실제 ETL 공통 함수.
    dag_id + task_name 기준으로 메타 1건 조회 후 ETL 수행.
    """

    run_id = run_id or f"manual__{datetime.now():%Y%m%dT%H%M%S}__{uuid.uuid4().hex[:8]}"
    task_id = task_id or task_name

    table_config = get_single_table_config(
        dag_id=dag_id,
        task_name=task_name,
        meta_file=meta_file,
        param_file=param_file,
        calendar_file=calendar_file,
        today_dt=today_dt,
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
    raw_input_param = table_config.get("input_param") or {}
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

    extract_row_count = 0
    stg_load_row_count = 0
    target_insert_count = 0
    target_update_count = 0
    target_delete_count = 0
    file_write_row_count = 0
    target_file_path = ""

    try:
        start_run_log(
            log_file=log_file,
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            task_name=meta_task_name,
            exec_seq=exec_seq,
            source_table=source_table,
            target_table=target_table,
            load_option=load_option,
            source_conn_name=source_conn_name,
            target_conn_name=target_conn_name,
            input_param=raw_input_param,
            config_option={
                **config_option,
                "TASK_ID": task_id,
                "TASK_NAME": meta_task_name,
                "EXEC_SEQ": str(exec_seq),
            },
        )

        input_params = parse_input_params(safe_json_dumps(raw_input_param))

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

        source_hook = get_postgres_hook(source_conn_name)
        target_hook = get_postgres_hook(target_conn_name)

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
                if hasattr(target_tx_conn, "autocommit"):
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

        finish_run_log(
            log_file=log_file,
            status="SUCCESS",
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            task_name=meta_task_name,
            exec_seq=exec_seq,
            source_table=source_table,
            target_table=target_table,
            extract_row_count=extract_row_count,
            stg_load_row_count=stg_load_row_count,
            target_insert_count=target_insert_count,
            target_update_count=target_update_count,
            target_delete_count=target_delete_count,
            file_write_row_count=file_write_row_count,
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

        finish_run_log(
            log_file=log_file,
            status="FAILED",
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            task_name=meta_task_name,
            exec_seq=exec_seq,
            source_table=source_table,
            target_table=target_table,
            extract_row_count=extract_row_count,
            stg_load_row_count=stg_load_row_count,
            target_insert_count=target_insert_count,
            target_update_count=target_update_count,
            target_delete_count=target_delete_count,
            file_write_row_count=file_write_row_count,
            error_message=traceback.format_exc(),
        )

        raise


# ****** standalone 전용 ******
def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run standalone PostgreSQL to PostgreSQL ETL with CSV metadata.")
    parser.add_argument("--dag-id", required=True, help="meta CSV dag_id")
    parser.add_argument("--task-name", required=True, help="meta CSV task_name")
    parser.add_argument("--today-dt", default=None, help="yyyyMMdd value for local params")
    parser.add_argument("--meta-file", default=DEFAULT_META_FILE, help="CSV file containing etl_meta_db_to_db-like metadata.")
    parser.add_argument("--param-file", default=DEFAULT_PARAM_FILE, help="Local JSON file replacing etl_param.")
    parser.add_argument("--calendar-file", default=DEFAULT_CALENDAR_FILE, help="Optional etl_calendar-like CSV file. If missing, dates are calculated.")
    parser.add_argument("--log-file", default=DEFAULT_LOG_FILE, help="JSONL run log file replacing etl_job_run_dtl_hist.")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, help="Fetch/insert chunk size. Default: 5000")
    parser.add_argument("--run-id", default=None, help="Optional run id for JSONL log.")
    parser.add_argument("--task-id", default=None, help="Optional task id for JSONL log.")
    return parser


# ****** standalone 전용 ******
def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    run_postgres_to_postgres_etl(
        dag_id=args.dag_id,
        task_name=args.task_name,
        meta_file=args.meta_file,
        param_file=args.param_file,
        calendar_file=args.calendar_file,
        log_file=args.log_file,
        today_dt=args.today_dt,
        chunk_size=args.chunk_size,
        run_id=args.run_id,
        task_id=args.task_id or args.task_name,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
