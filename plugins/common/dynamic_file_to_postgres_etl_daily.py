from __future__ import annotations

import csv
import json
import subprocess
import traceback
from pathlib import Path

from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.etl_hist_utils import (
    get_task_runtime_info,
    insert_etl_run_hist,
    update_etl_run_hist_success,
    update_etl_run_hist_failed,
)


DEFAULT_META_POSTGRES_CONN_ID = "postgres_conn"


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
        key = str(k).strip().upper()
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
        f"Unsupported source_file_encoding: {raw_encoding}. "
        f"Allowed values are utf-8, euc-kr, cp949."
    )


def read_file_all_rows(
    file_path: str,
    file_type: str,
    text_source_column: str = "line_text",
    csv_file_delimiter: str = ",",
    encoding: str = "utf-8",
) -> tuple[list[str], list[tuple]]:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    file_type = file_type.lower().strip()

    if file_type == "csv":
        delimiter = normalize_csv_delimiter(csv_file_delimiter)

        with path.open("r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)

            if not reader.fieldnames:
                raise ValueError(f"CSV header not found: {file_path}")

            source_columns = [str(c).strip() for c in reader.fieldnames]

            all_rows = [
                tuple(row.get(col) for col in source_columns)
                for row in reader
            ]

        return source_columns, all_rows

    if file_type == "json":
        with path.open("r", encoding=encoding) as f:
            content = f.read().strip()

        if not content:
            raise ValueError(f"JSON file is empty: {file_path}")

        if content.startswith("["):
            obj = json.loads(content)

            if not isinstance(obj, list):
                raise ValueError(f"JSON file must be array: {file_path}")

            if not obj:
                return [], []

            first_row = obj[0]

            if not isinstance(first_row, dict):
                raise ValueError(
                    f"JSON array row must be object(dict): {file_path}"
                )

            source_columns = [str(k).strip() for k in first_row.keys()]
            all_rows = []

            for row in obj:
                if not isinstance(row, dict):
                    raise ValueError(
                        f"JSON array row must be object(dict): {file_path}"
                    )

                all_rows.append(tuple(row.get(col) for col in source_columns))

            return source_columns, all_rows

        lines = [line.strip() for line in content.splitlines() if line.strip()]

        if not lines:
            return [], []

        first_obj = json.loads(lines[0])

        if not isinstance(first_obj, dict):
            raise ValueError(
                f"NDJSON row must be object(dict): {file_path}"
            )

        source_columns = [str(k).strip() for k in first_obj.keys()]
        all_rows = []

        for line in lines:
            row = json.loads(line)

            if not isinstance(row, dict):
                raise ValueError(
                    f"NDJSON row must be object(dict): {file_path}"
                )

            all_rows.append(tuple(row.get(col) for col in source_columns))

        return source_columns, all_rows

    if file_type == "text":
        source_columns = [text_source_column]

        with path.open("r", encoding=encoding) as f:
            all_rows = [(line.rstrip("\r\n"),) for line in f]

        return source_columns, all_rows

    raise ValueError(
        f"Unsupported source_file_type: {file_type}. "
        f"Allowed values are json, csv, text."
    )


def create_file_to_postgres_meta_v2_tasks(
    dag_id: str,
    meta_postgres_conn_id: str = DEFAULT_META_POSTGRES_CONN_ID,
):
    """
    File -> PostgreSQL Meta V2 기반 ETL 공통 Task 생성 함수.

    V2 특징:
    - ETL_PARAM INSERT 없음
    - 기존 ETL_PARAM 최신 값을 기준으로 etl_meta_file_to_db.input_param UPDATE만 수행
    """

    @task(task_id="get_table_configs")
    def get_table_configs():
        meta_hook = PostgresHook(postgres_conn_id=meta_postgres_conn_id)

        update_input_param_sql = """
        UPDATE etl_meta_file_to_db a
        SET input_param = b.tobe_param
        FROM (
            SELECT dag_id, task_name, tobe_param
            FROM etl_param a
            WHERE dag_id = %s
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
            column_mapping,
            load_option,
            stg_drop_yn,
            source_file_type,
            csv_file_delimiter,
            source_file_encoding,
            source_file_dir,
            source_pre_cmd,
            target_pre_sql,
            target_post_sql,
            config_option,
            input_param
        FROM etl_meta_file_to_db
        WHERE 1=1
          AND enable_yn = 'Y'
          AND dag_id = %s
          AND disable_dt = '20991231'
        ORDER BY
            COALESCE(exec_seq, 999999),
            task_name,
            source_table,
            target_table
        """

        conn = None
        cursor = None

        try:
            conn = meta_hook.get_conn()
            conn.autocommit = False
            cursor = conn.cursor()

            cursor.execute("SET TIME ZONE 'Asia/Seoul'")

            cursor.execute(update_input_param_sql, (dag_id,))
            cursor.execute(select_meta_sql, (dag_id,))
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
            task_name = (r[0] or "").strip() if r[0] is not None else ""
            exec_seq = int(r[1]) if r[1] is not None else 999999

            source_table = (r[2] or "").strip() if r[2] is not None else ""
            target_table = (r[3] or "").strip() if r[3] is not None else ""
            pk_columns = parse_csv_columns(r[4])
            column_mapping = r[5]
            load_option = (
                (r[6] or "di").strip().lower()
                if r[6] is not None
                else "di"
            )
            stg_drop_yn = (
                (r[7] or "N").strip().upper()
                if r[7] is not None
                else "N"
            )
            source_file_type = (
                (r[8] or "").strip().lower()
                if r[8] is not None
                else ""
            )
            csv_file_delimiter = (r[9] or ",") if r[9] is not None else ","
            source_file_encoding = (
                (r[10] or "utf-8").strip()
                if r[10] is not None
                else "utf-8"
            )
            source_file_dir = (r[11] or "").strip() if r[11] is not None else ""
            source_pre_cmd = (r[12] or "").strip() if r[12] is not None else ""
            target_pre_sql = (r[13] or "").strip() if r[13] is not None else ""
            target_post_sql = (r[14] or "").strip() if r[14] is not None else ""
            config_option = (r[15] or "").strip() if r[15] is not None else ""
            input_param = (r[16] or "").strip() if r[16] is not None else ""

            if not task_name:
                task_name = f"{source_table}_to_{target_table}"

            normalized_source_file_encoding = normalize_file_encoding(
                source_file_encoding
            )

            parsed_config_option = parse_config_option(config_option)
            target_conn_name = (
                parsed_config_option.get("TARGET_CONN_NAME") or ""
            ).strip()

            if not source_table:
                raise ValueError(
                    "etl_meta_file_to_db.source_table(file_name/pattern) is empty"
                )

            if not target_table:
                raise ValueError("etl_meta_file_to_db.target_table is empty")

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

            if source_file_type not in ("json", "csv", "text"):
                raise ValueError(
                    f"{target_table}: invalid source_file_type [{source_file_type}]. "
                    f"Allowed values are json, csv, text."
                )

            if source_file_type == "csv" and not csv_file_delimiter:
                raise ValueError(
                    f"{target_table}: csv_file_delimiter is empty for csv source"
                )

            if not source_file_dir:
                raise ValueError(
                    f"{target_table}: source_file_dir is empty"
                )

            if load_option in ("ui", "di", "u", "d") and not pk_columns:
                raise ValueError(
                    f"{target_table}: pk_column is required for load_option [{load_option}]"
                )

            if not target_conn_name:
                raise ValueError(
                    f"{target_table}: config_option.TARGET_CONN_NAME is empty"
                )

            configs.append(
                {
                    "task_name": task_name,
                    "exec_seq": exec_seq,
                    "source_table": source_table,
                    "target_table": target_table,
                    "pk_columns": pk_columns,
                    "column_mapping": column_mapping,
                    "load_option": load_option,
                    "stg_drop_yn": stg_drop_yn,
                    "source_file_type": source_file_type,
                    "csv_file_delimiter": csv_file_delimiter,
                    "source_file_encoding": normalized_source_file_encoding,
                    "source_file_dir": source_file_dir,
                    "source_pre_cmd": source_pre_cmd,
                    "target_pre_sql": target_pre_sql,
                    "target_post_sql": target_post_sql,
                    "config_option": parsed_config_option,
                    "input_param": input_param,
                    "target_conn_name": target_conn_name,
                }
            )

        print(f"Loaded File to PostgreSQL V2 ETL configs. dag_id={dag_id}")

        for idx, cfg in enumerate(configs):
            print(
                f"map_index={idx}, "
                f"exec_seq={cfg.get('exec_seq')}, "
                f"task_name={cfg.get('task_name')}, "
                f"source_table={cfg.get('source_table')}, "
                f"target_table={cfg.get('target_table')}"
            )

        return configs

    @task(task_id="run_etl", pool_slots=1)
    def run_etl(table_config: dict, **context):
        runtime_info = get_task_runtime_info(**context)

        task_name = (table_config.get("task_name") or "").strip()
        exec_seq = table_config.get("exec_seq")

        source_table = (table_config.get("source_table") or "").strip()
        target_table = (table_config.get("target_table") or "").strip()
        pk_columns = table_config.get("pk_columns") or []
        raw_column_mapping = table_config.get("column_mapping")
        load_option = (table_config.get("load_option") or "di").strip().lower()
        stg_drop_yn = (table_config.get("stg_drop_yn") or "N").strip().upper()

        raw_source_file_type = (
            table_config.get("source_file_type") or ""
        ).strip().lower()
        raw_csv_file_delimiter = str(
            table_config.get("csv_file_delimiter") or ","
        )
        raw_source_file_encoding = str(
            table_config.get("source_file_encoding") or "utf-8"
        )
        raw_source_file_dir = (
            table_config.get("source_file_dir") or ""
        ).strip()
        raw_source_pre_cmd = (
            table_config.get("source_pre_cmd") or ""
        ).strip()
        raw_target_pre_sql = (
            table_config.get("target_pre_sql") or ""
        ).strip()
        raw_target_post_sql = (
            table_config.get("target_post_sql") or ""
        ).strip()
        raw_input_param = table_config.get("input_param")

        config_option = table_config.get("config_option") or {}
        target_conn_name = (
            table_config.get("target_conn_name") or ""
        ).strip()

        print(
            f"START ETL "
            f"dag_id={dag_id}, "
            f"task_name={task_name}, "
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

        if not source_table:
            raise ValueError(
                "table_config.source_table(file_name/pattern) is empty"
            )

        if not target_table:
            raise ValueError("table_config.target_table is empty")

        if load_option not in ("ui", "di", "ti", "i", "u", "d"):
            raise ValueError(
                f"{target_table}: invalid load_option [{load_option}]"
            )

        if stg_drop_yn not in ("Y", "N"):
            raise ValueError(
                f"{target_table}: invalid stg_drop_yn [{stg_drop_yn}]"
            )

        if raw_source_file_type not in ("json", "csv", "text"):
            raise ValueError(
                f"{target_table}: invalid source_file_type [{raw_source_file_type}]"
            )

        if not raw_source_file_dir:
            raise ValueError(f"{target_table}: source_file_dir is empty")

        if load_option in ("ui", "di", "u", "d") and not pk_columns:
            raise ValueError(
                f"{target_table}: pk_columns is required for load_option [{load_option}]"
            )

        try:
            if not target_conn_name:
                raise ValueError(
                    f"{target_table}: config_option.TARGET_CONN_NAME is empty"
                )

            run_hist_id = insert_etl_run_hist(
                dag_id=dag_id,
                run_id=runtime_info["run_id"],
                task_id=task_name or runtime_info["task_id"],
                map_index=runtime_info["map_index"],
                source_table=source_table,
                target_table=target_table,
                load_option=load_option,
                source_conn_name="",
                target_conn_name=target_conn_name,
                input_param=raw_input_param,
                config_option={
                    **config_option,
                    "AIRFLOW_TASK_ID": runtime_info["task_id"],
                    "TASK_NAME": task_name,
                    "EXEC_SEQ": str(exec_seq),
                },
            )

            input_params = parse_input_params(raw_input_param)

            source_file_type = apply_input_params(
                raw_source_file_type,
                input_params,
            ).lower()

            csv_file_delimiter = apply_input_params(
                raw_csv_file_delimiter,
                input_params,
            )

            normalized_csv_file_delimiter = normalize_csv_delimiter(
                csv_file_delimiter
            )

            source_file_encoding = apply_input_params(
                raw_source_file_encoding,
                input_params,
            )

            normalized_source_file_encoding = normalize_file_encoding(
                source_file_encoding
            )

            source_file_dir = apply_input_params(
                raw_source_file_dir,
                input_params,
            )

            source_pre_cmd = apply_input_params(
                raw_source_pre_cmd,
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

            source_file_pattern = apply_input_params(
                source_table,
                input_params,
            )

            if source_file_type == "csv" and not normalized_csv_file_delimiter:
                raise ValueError(
                    f"{target_table}: csv_file_delimiter is empty after param replacement"
                )

            source_dir_path = Path(source_file_dir)
            matched_files = sorted(source_dir_path.glob(source_file_pattern))

            print(f"[DEBUG] dag_id={dag_id}")
            print(f"[DEBUG] target_conn_name={target_conn_name}")
            print(f"[DEBUG] source_file_dir={source_file_dir}")
            print(f"[DEBUG] source_file_pattern={source_file_pattern}")
            print(f"[DEBUG] matched_files={[str(p) for p in matched_files]}")
            print(f"[DEBUG] matched_file_count={len(matched_files)}")
            print(f"[DEBUG] source_file_type={source_file_type}")
            print(f"[DEBUG] csv_file_delimiter_raw={csv_file_delimiter}")
            print(
                f"[DEBUG] csv_file_delimiter_normalized="
                f"{repr(normalized_csv_file_delimiter)}"
            )
            print(f"[DEBUG] source_file_encoding_raw={source_file_encoding}")
            print(
                f"[DEBUG] source_file_encoding_normalized="
                f"{normalized_source_file_encoding}"
            )

            if not matched_files:
                raise FileNotFoundError(
                    f"{target_table}: no source files matched. "
                    f"source_file_dir=[{source_file_dir}], "
                    f"source_table(pattern)=[{source_file_pattern}]"
                )

            stg_table = f"stg_{target_table}"

            create_stg_sql = f"""
                CREATE TABLE IF NOT EXISTS {stg_table}
                (LIKE {target_table} INCLUDING ALL)
            """

            truncate_stg_sql = f"TRUNCATE TABLE {stg_table}"
            truncate_target_sql = f"TRUNCATE TABLE {target_table}"
            drop_stg_sql = f"DROP TABLE IF EXISTS {stg_table}"

            target_hook = PostgresHook(postgres_conn_id=target_conn_name)

            target_tx_conn = None
            target_tx_cursor = None

            job_succeeded = False

            try:
                if source_pre_cmd:
                    completed = subprocess.run(
                        source_pre_cmd,
                        shell=True,
                        check=True,
                        capture_output=True,
                        text=True,
                    )

                    if completed.stdout:
                        print(completed.stdout)

                    if completed.stderr:
                        print(completed.stderr)

                target_hook.run(create_stg_sql)
                target_hook.run(truncate_stg_sql)

                column_mapping = parse_column_mapping(raw_column_mapping) or {}

                if source_file_type == "text":
                    if column_mapping:
                        if len(column_mapping) != 1:
                            raise ValueError(
                                f"{target_table}: text file requires exactly one column mapping"
                            )

                        text_source_column = list(column_mapping.keys())[0]

                    else:
                        text_source_column = "line_text"

                else:
                    text_source_column = "line_text"

                expected_source_columns = None
                target_columns = None

                for file_path in matched_files:
                    source_columns, all_rows = read_file_all_rows(
                        file_path=str(file_path),
                        file_type=source_file_type,
                        text_source_column=text_source_column,
                        csv_file_delimiter=normalized_csv_file_delimiter,
                        encoding=normalized_source_file_encoding,
                    )

                    print(f"[DEBUG] current_file={str(file_path)}")
                    print(f"[DEBUG] source_columns={source_columns}")
                    print(f"[DEBUG] total_read_rows={len(all_rows)}")
                    print(f"[DEBUG] sample_rows={all_rows[:5]}")

                    if not source_columns and not all_rows:
                        print(
                            f"{target_table}: source file has no rows [{file_path}]"
                        )
                        continue

                    if source_file_type in ("csv", "json") and not source_columns:
                        raise ValueError(
                            f"{target_table}: no source columns detected from file [{file_path}]"
                        )

                    if expected_source_columns is None:
                        expected_source_columns = source_columns

                        target_columns = [
                            column_mapping.get(src_col, src_col)
                            for src_col in expected_source_columns
                        ]

                        if not target_columns and all_rows:
                            raise ValueError(
                                f"{target_table}: mapped target_columns is empty"
                            )

                        if len(set(target_columns)) != len(target_columns):
                            raise ValueError(
                                f"{target_table}: duplicate target columns detected after mapping. "
                                f"source_columns={expected_source_columns}, "
                                f"target_columns={target_columns}"
                            )

                        missing_pk_columns = [
                            pk for pk in pk_columns if pk not in target_columns
                        ]

                        if missing_pk_columns:
                            raise ValueError(
                                f"{target_table}: mapped result does not include PK columns: "
                                f"{missing_pk_columns}. "
                                f"source_columns={expected_source_columns}, "
                                f"target_columns={target_columns}"
                            )

                    else:
                        if source_columns != expected_source_columns:
                            raise ValueError(
                                f"{target_table}: source columns mismatch across files. "
                                f"expected={expected_source_columns}, "
                                f"current={source_columns}, "
                                f"file={file_path}"
                            )

                    if all_rows:
                        target_hook.insert_rows(
                            table=stg_table,
                            rows=all_rows,
                            target_fields=target_columns,
                            commit_every=max(1, len(all_rows)),
                            executemany=True,
                        )

                        stg_count = target_hook.get_first(
                            f"SELECT COUNT(*) FROM {stg_table}"
                        )[0]

                        extract_row_count += len(all_rows)
                        stg_load_row_count += len(all_rows)

                        print(
                            f"{file_path} -> {stg_table} "
                            f"file_rows={len(all_rows)} "
                            f"total_rows={stg_load_row_count} "
                            f"stg_count_after_insert={stg_count} "
                            f"source_columns={source_columns} "
                            f"target_columns={target_columns}"
                        )

                if stg_load_row_count > 0:
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
                            print(
                                f"{stg_table} -> {target_table} INSERT completed (UI), "
                                f"total={stg_load_row_count}"
                            )

                        elif load_option == "di":
                            target_tx_cursor.execute(delete_sql)
                            target_delete_count = target_tx_cursor.rowcount
                            print(f"{target_table} DELETE completed")

                            target_tx_cursor.execute(insert_sql)
                            target_insert_count = target_tx_cursor.rowcount
                            print(
                                f"{stg_table} -> {target_table} INSERT completed (DI), "
                                f"total={stg_load_row_count}"
                            )

                        elif load_option == "ti":
                            target_tx_cursor.execute(truncate_target_sql)
                            print(f"{target_table} TRUNCATE completed")

                            target_tx_cursor.execute(insert_sql)
                            target_insert_count = target_tx_cursor.rowcount
                            print(
                                f"{stg_table} -> {target_table} INSERT completed (TI), "
                                f"total={stg_load_row_count}"
                            )

                        elif load_option == "i":
                            target_tx_cursor.execute(insert_sql)
                            target_insert_count = target_tx_cursor.rowcount
                            print(
                                f"{stg_table} -> {target_table} INSERT completed (I), "
                                f"total={stg_load_row_count}"
                            )

                        elif load_option == "u":
                            if not update_sql:
                                print(
                                    f"{target_table}: no non-pk columns to update, UPDATE skipped (U)"
                                )
                            else:
                                target_tx_cursor.execute(update_sql)
                                target_update_count = target_tx_cursor.rowcount
                                print(f"{target_table} UPDATE completed (U)")

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
                    print(
                        f"{target_table}: no rows read from matched files, target load skipped"
                    )
                    job_succeeded = True

            finally:
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
                f"task_name={task_name}, "
                f"exec_seq={exec_seq}, "
                f"source_table={source_table}, "
                f"target_table={target_table}"
            )

        except Exception:
            print(
                f"END ETL FAILED "
                f"dag_id={dag_id}, "
                f"task_name={task_name}, "
                f"exec_seq={exec_seq}, "
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

    table_configs = get_table_configs()
    run_etl_tasks = run_etl.expand(table_config=table_configs)

    return table_configs, run_etl_tasks