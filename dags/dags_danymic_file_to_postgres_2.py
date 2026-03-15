from __future__ import annotations

import csv
import json
import subprocess
from datetime import datetime
from pathlib import Path


from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_ID = "dynamic_file_to_postgres_etl_meta_2"

TARGET_POSTGRES_CONN_ID = "postgres_conn"
META_POSTGRES_CONN_ID = "postgres_conn"



def parse_csv_columns(raw_value: str | None) -> list[str]:
    if raw_value is None:
        return []

    return [c.strip() for c in raw_value.split(",") if c and c.strip()]


def parse_column_mapping(raw_mapping: str | None) -> dict[str, str]:
    """
    source column -> target column 매핑
    지원 형식:
      1) JSON dict
         {"src_a":"a","src_b":"b"}

      2) JSON list
         [{"source":"src_a","target":"a"},{"source":"src_b","target":"b"}]

      3) 문자열
         "src_a:a, src_b:b"
         "src_a=a, src_b=b"
    """
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


def read_file_all_rows(
    file_path: str,
    file_type: str,
    text_source_column: str = "line_text",
    encoding: str = "utf-8",
) -> tuple[list[str], list[tuple]]:
    """
    파일 전체를 한 번에 읽어서
    (source_columns, all_rows) 반환

    지원:
      - csv  : header 필수
      - json : JSON array 또는 NDJSON(JSON lines)
      - text : 1 line = 1 row, 단일 컬럼(text_source_column)
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {file_path}")

    file_type = file_type.lower().strip()

    if file_type == "csv":
        with path.open("r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f)

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

        # JSON Array
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

        # NDJSON
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

        update_input_param_sql = """
        UPDATE etl_meta_file_to_db a
        SET input_param = b.tobe_param
        FROM (
            SELECT dag_id, tobe_param
            FROM (
                SELECT dag_id, tobe_param
                FROM etl_param
                WHERE dag_id = %s
                ORDER BY created_tm DESC
            ) x
            LIMIT 1
        ) b
        WHERE a.dag_id = b.dag_id
        """

        select_meta_sql = """
        SELECT
            source_table,
            target_table,
            pk_column,
            column_mapping,
            load_option,
            stg_drop_yn,
            source_file_type,
            source_file_dir,
            source_pre_cmd,
            target_pre_sql,
            target_post_sql,
            input_param
        FROM etl_meta_file_to_db
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
            pk_columns = parse_csv_columns(r[2])
            column_mapping = r[3]
            load_option = (r[4] or "di").strip().lower() if r[4] is not None else "di"
            stg_drop_yn = (r[5] or "N").strip().upper() if r[5] is not None else "N"
            source_file_type = (r[6] or "").strip().lower() if r[6] is not None else ""
            source_file_dir = (r[7] or "").strip() if r[7] is not None else ""
            source_pre_cmd = (r[8] or "").strip() if r[8] is not None else ""
            target_pre_sql = (r[9] or "").strip() if r[9] is not None else ""
            target_post_sql = (r[10] or "").strip() if r[10] is not None else ""
            input_param = (r[11] or "").strip() if r[11] is not None else ""

            if not source_table:
                raise ValueError("etl_meta_file_to_db.source_table(file_name) is empty")

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

            if not source_file_dir:
                raise ValueError(
                    f"{target_table}: source_file_dir is empty"
                )

            if load_option in ("ui", "di", "u", "d") and not pk_columns:
                raise ValueError(
                    f"{target_table}: pk_column is required for load_option [{load_option}]"
                )

            configs.append(
                {
                    "source_table": source_table,   # 파일명
                    "target_table": target_table,
                    "pk_columns": pk_columns,
                    "column_mapping": column_mapping,
                    "load_option": load_option,
                    "stg_drop_yn": stg_drop_yn,
                    "source_file_type": source_file_type,
                    "source_file_dir": source_file_dir,  # 디렉터리 경로
                    "source_pre_cmd": source_pre_cmd,
                    "target_pre_sql": target_pre_sql,
                    "target_post_sql": target_post_sql,
                    "input_param": input_param,
                }
            )

        return configs

    @task(pool_slots=1)
    def run_etl(table_config: dict):
        source_table = (table_config.get("source_table") or "").strip()   # 파일명
        target_table = (table_config.get("target_table") or "").strip()
        pk_columns = table_config.get("pk_columns") or []
        raw_column_mapping = table_config.get("column_mapping")
        load_option = (table_config.get("load_option") or "di").strip().lower()
        stg_drop_yn = (table_config.get("stg_drop_yn") or "N").strip().upper()

        raw_source_file_type = (table_config.get("source_file_type") or "").strip().lower()
        raw_source_file_dir = (table_config.get("source_file_dir") or "").strip()
        raw_source_pre_cmd = (table_config.get("source_pre_cmd") or "").strip()
        raw_target_pre_sql = (table_config.get("target_pre_sql") or "").strip()
        raw_target_post_sql = (table_config.get("target_post_sql") or "").strip()
        raw_input_param = table_config.get("input_param")

        if not source_table:
            raise ValueError("table_config.source_table(file_name) is empty")

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

        input_params = parse_input_params(raw_input_param)

        source_file_type = apply_input_params(raw_source_file_type, input_params).lower()
        source_file_dir = apply_input_params(raw_source_file_dir, input_params)
        source_pre_cmd = apply_input_params(raw_source_pre_cmd, input_params)
        target_pre_sql = apply_input_params(raw_target_pre_sql, input_params)
        target_post_sql = apply_input_params(raw_target_post_sql, input_params)

        source_file_name = apply_input_params(source_table, input_params)

        full_file_path = str(Path(source_file_dir) / source_file_name)

        print(f"[DEBUG] source_file_dir={source_file_dir}")
        print(f"[DEBUG] source_file_name={source_file_name}")
        print(f"[DEBUG] full_file_path={full_file_path}")
        print(f"[DEBUG] file_exists={Path(full_file_path).exists()}")

        stg_table = f"stg_{target_table}"

        create_stg_sql = f"""
            CREATE TABLE IF NOT EXISTS {stg_table}
            (LIKE {target_table} INCLUDING ALL)
        """

        truncate_stg_sql = f"TRUNCATE TABLE {stg_table}"
        truncate_target_sql = f"TRUNCATE TABLE {target_table}"
        drop_stg_sql = f"DROP TABLE IF EXISTS {stg_table}"

        target_hook = PostgresHook(postgres_conn_id=TARGET_POSTGRES_CONN_ID)

        target_tx_conn = None
        target_tx_cursor = None

        total_rows = 0
        job_succeeded = False

        try:
            # 1) source file 읽기 전 OS command 수행
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

            # 2) STG 준비
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

            # 3) 파일 전체 읽기
            source_columns, all_rows = read_file_all_rows(
                file_path=full_file_path,
                file_type=source_file_type,
                text_source_column=text_source_column,
            )

            print(f"[DEBUG] source_columns={source_columns}")
            print(f"[DEBUG] total_read_rows={len(all_rows)}")
            print(f"[DEBUG] sample_rows={all_rows[:5]}")

            if not source_columns and not all_rows:
                print(f"{target_table}: source file has no rows [{full_file_path}]")
                source_columns = []

            if source_file_type in ("csv", "json") and not source_columns:
                raise ValueError(
                    f"{target_table}: no source columns detected from file [{full_file_path}]"
                )

            target_columns = [
                column_mapping.get(src_col, src_col)
                for src_col in source_columns
            ]

            if not target_columns and all_rows:
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

            # 4) 파일 전체 -> STG 1회 적재
            if all_rows:
                target_hook.insert_rows(
                    table=stg_table,
                    rows=all_rows,
                    target_fields=target_columns,
                    commit_every=max(1, len(all_rows)),
                    executemany=True,
                )

                stg_count = target_hook.get_first(f"SELECT COUNT(*) FROM {stg_table}")[0]
                print(f"[DEBUG] stg_count_after_insert={stg_count}")

                total_rows = len(all_rows)

                print(
                    f"{full_file_path} -> {stg_table} "
                    f"total={total_rows} "
                    f"source_columns={source_columns} "
                    f"target_columns={target_columns}"
                )

            # 5) STG -> TARGET
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
                print(f"{target_table}: no rows read from file, target load skipped")
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

    table_configs = get_table_configs()
    run_etl.expand(table_config=table_configs)