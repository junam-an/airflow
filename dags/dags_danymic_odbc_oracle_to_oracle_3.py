from __future__ import annotations

import json
import traceback
from datetime import datetime

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


DAG_ID = "DYNAMIC_ORACLE_TO_ORACLE_ETL_META_3"


# 메타 테이블 저장소
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
        raise ValueError(f"Invalid config_option JSON: {raw_config_option}") from e

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
        WHERE 1 = 0
    """


def build_create_stg_pk_sql(stg_table: str, pk_columns: list[str]) -> str | None:
    if not pk_columns:
        return None

    pk_columns_sql = ", ".join(pk_columns)
    pk_name = f"PK_{stg_table.upper()}"[:30]

    return f"""
        ALTER TABLE {stg_table}
        ADD CONSTRAINT {pk_name}
        PRIMARY KEY ({pk_columns_sql})
    """


def build_insert_sql(target_table: str, stg_table: str, target_columns: list[str]) -> str:
    insert_columns_sql = ", ".join(target_columns)
    select_columns_sql = ", ".join([f"s.{col}" for col in target_columns])

    return f"""
        INSERT INTO {target_table} ({insert_columns_sql})
        SELECT {select_columns_sql}
        FROM {stg_table} s
    """



def build_delete_sql(target_table: str, stg_table: str, pk_columns: list[str]) -> str:
    pk_join_condition_sql = " AND ".join(
        [f"t.{pk} = s.{pk}" for pk in pk_columns]
    )

    return f"""
        DELETE FROM {target_table} t
        WHERE EXISTS (
            SELECT 1
            FROM {stg_table} s
            WHERE {pk_join_condition_sql}
        )
    """


def build_merge_ui_sql(
    target_table: str,
    stg_table: str,
    target_columns: list[str],
    pk_columns: list[str],
) -> str:
    pk_join_condition_sql = " AND ".join(
        [f"t.{pk} = s.{pk}" for pk in pk_columns]
    )

    non_pk_columns = [c for c in target_columns if c not in pk_columns]

    update_clause = ""
    if non_pk_columns:
        update_set_sql = ", ".join(
            [f"t.{col} = s.{col}" for col in non_pk_columns]
        )
        update_clause = f"WHEN MATCHED THEN UPDATE SET {update_set_sql}"

    insert_columns_sql = ", ".join(target_columns)
    insert_values_sql = ", ".join([f"s.{col}" for col in target_columns])

    return f"""
        MERGE INTO {target_table} t
        USING {stg_table} s
           ON ({pk_join_condition_sql})
        {update_clause}
        WHEN NOT MATCHED THEN
             INSERT ({insert_columns_sql})
             VALUES ({insert_values_sql})
    """


def build_merge_update_sql(
    target_table: str,
    stg_table: str,
    target_columns: list[str],
    pk_columns: list[str],
) -> str | None:
    non_pk_columns = [c for c in target_columns if c not in pk_columns]

    if not non_pk_columns:
        return None

    pk_join_condition_sql = " AND ".join(
        [f"t.{pk} = s.{pk}" for pk in pk_columns]
    )
    update_set_sql = ", ".join(
        [f"t.{col} = s.{col}" for col in non_pk_columns]
    )

    return f"""
        MERGE INTO {target_table} t
        USING {stg_table} s
           ON ({pk_join_condition_sql})
        WHEN MATCHED THEN
             UPDATE SET {update_set_sql}
    """


def build_stg_insert_sql(stg_table: str, target_columns: list[str]) -> str:
    columns_sql = ", ".join(target_columns)
    bind_sql = ", ".join(["?"] * len(target_columns))

    return f"""
        INSERT INTO {stg_table} ({columns_sql})
        VALUES ({bind_sql})
    """


def normalize_rows_for_odbc(rows: list[tuple], column_count: int) -> list[tuple]:
    normalized = []

    for row in rows:
        if row is None:
            continue

        new_row = tuple(row)

        if len(new_row) != column_count:
            raise ValueError(
                f"Row column count mismatch. expected={column_count}, actual={len(new_row)}, row={new_row}"
            )

        normalized.append(new_row)

    return normalized


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

        meta_hook.run("SET TIME ZONE 'Asia/Seoul'")

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
        FROM ETL_META_DB_TO_DB A, BASE_PARAM B
        WHERE 1=1
        AND DAG_ID = %s
        AND DISABLE_DT = '20991231'
        AND ENABLE_YN = 'Y'
        """

        update_input_param_sql = """
        UPDATE etl_meta_db_to_db a
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
            pk_columns = parse_csv_columns(r[2])
            source_exec_sql = (r[3] or "").strip() if r[3] is not None else ""
            column_mapping = r[4]
            load_option = (r[5] or "di").strip().lower() if r[5] is not None else "di"
            stg_drop_yn = (r[6] or "N").strip().upper() if r[6] is not None else "N"
            target_pre_sql = (r[7] or "").strip() if r[7] is not None else ""
            target_post_sql = (r[8] or "").strip() if r[8] is not None else ""
            config_option = (r[9] or "").strip() if r[9] is not None else ""
            input_param = (r[10] or "").strip() if r[10] is not None else ""

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

            parsed_config = parse_config_option(config_option)

            source_conn_name = parsed_config.get("SOURCE_CONN_NAME", "").strip()
            target_conn_name = parsed_config.get("TARGET_CONN_NAME", "").strip()

            if not source_conn_name:
                raise ValueError(
                    f"{target_table}: config_option.SOURCE_CONN_NAME is empty"
                )

            if not target_conn_name:
                raise ValueError(
                    f"{target_table}: config_option.TARGET_CONN_NAME is empty"
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
                    "config_option": parsed_config,
                    "input_param": input_param,
                }
            )

        return configs

    @task(pool_slots=1)
    def run_etl(table_config: dict, **context):
        runtime_info = get_task_runtime_info(**context)

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
        config_option = table_config.get("config_option") or {}

        run_hist_id = None
        extract_row_count = 0
        stg_load_row_count = 0
        target_insert_count = 0
        target_update_count = 0
        target_delete_count = 0
        file_write_row_count = 0
        target_file_path = ""

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

        source_conn_name = str(config_option.get("SOURCE_CONN_NAME", "")).strip()
        target_conn_name = str(config_option.get("TARGET_CONN_NAME", "")).strip()

        if not source_conn_name:
            raise ValueError(f"{target_table}: SOURCE_CONN_NAME is empty")

        if not target_conn_name:
            raise ValueError(f"{target_table}: TARGET_CONN_NAME is empty")

        try:
            run_hist_id = insert_etl_run_hist(
                dag_id=DAG_ID,
                run_id=runtime_info["run_id"],
                task_id=runtime_info["task_id"],
                map_index=runtime_info["map_index"],
                source_table=source_table,
                target_table=target_table,
                load_option=load_option,
                source_conn_name=source_conn_name,
                target_conn_name=target_conn_name,
                input_param=raw_input_param,
                config_option=config_option,
            )

            input_params = parse_input_params(raw_input_param)

            source_exec_sql = apply_input_params(raw_source_exec_sql, input_params)
            target_pre_sql = apply_input_params(raw_target_pre_sql, input_params)
            target_post_sql = apply_input_params(raw_target_post_sql, input_params)

            if not source_exec_sql:
                raise ValueError(
                    f"{target_table}: source_exec_sql is empty after param replacement"
                )

            stg_table = f"STG_{target_table}"

            drop_stg_sql = f"DROP TABLE {stg_table}"
            create_stg_sql = f"""
                CREATE TABLE {stg_table}
                AS
                SELECT *
                FROM {target_table}
                WHERE 1 = 0
            """
            create_stg_pk_sql = build_create_stg_pk_sql(
                stg_table=stg_table,
                pk_columns=pk_columns,
            )
            truncate_target_sql = f"TRUNCATE TABLE {target_table}"

            source_hook = OdbcHook(odbc_conn_id=source_conn_name)
            target_hook = OdbcHook(odbc_conn_id=target_conn_name)

            source_conn = None
            meta_cursor = None
            source_cursor = None

            target_stg_conn = None
            target_stg_cursor = None

            target_tx_conn = None
            target_tx_cursor = None

            job_succeeded = False

            try:
                try:
                    target_hook.run(drop_stg_sql)
                    print(f"{stg_table} dropped before recreate")
                except Exception:
                    print(f"{stg_table} drop skipped (not exists or drop failed before recreate)")

                target_hook.run(create_stg_sql)
                print(f"{stg_table} created")

                if create_stg_pk_sql:
                    target_hook.run(create_stg_pk_sql)
                    print(f"{stg_table} primary key created: {pk_columns}")
                else:
                    print(f"{stg_table} primary key creation skipped (no pk_columns)")

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

                insert_sql = build_insert_sql(target_table, stg_table, target_columns)

                delete_sql = build_delete_sql(
                    target_table=target_table,
                    stg_table=stg_table,
                    pk_columns=pk_columns,
                )
                merge_ui_sql = build_merge_ui_sql(
                    target_table=target_table,
                    stg_table=stg_table,
                    target_columns=target_columns,
                    pk_columns=pk_columns,
                )
                merge_update_sql = build_merge_update_sql(
                    target_table=target_table,
                    stg_table=stg_table,
                    target_columns=target_columns,
                    pk_columns=pk_columns,
                )

                stg_insert_sql = build_stg_insert_sql(stg_table, target_columns)

                target_stg_conn = target_hook.get_conn()
                target_stg_conn.autocommit = False
                target_stg_cursor = target_stg_conn.cursor()

                try:
                    target_stg_cursor.fast_executemany = True
                except Exception:
                    pass

                source_cursor = source_conn.cursor()
                source_cursor.arraysize = CHUNK_SIZE
                source_cursor.execute(source_exec_sql)

                while True:
                    rows = source_cursor.fetchmany(CHUNK_SIZE)

                    if not rows:
                        break

                    normalized_rows = normalize_rows_for_odbc(
                        rows=rows,
                        column_count=len(target_columns),
                    )

                    if not normalized_rows:
                        continue

                    target_stg_cursor.executemany(stg_insert_sql, normalized_rows)
                    target_stg_conn.commit()

                    extract_row_count += len(normalized_rows)
                    stg_load_row_count += len(normalized_rows)

                    print(
                        f"{source_table or '[source_sql]'} -> {stg_table} "
                        f"chunk={len(normalized_rows)} total={stg_load_row_count} "
                        f"source_columns={source_columns} "
                        f"target_columns={target_columns}"
                    )

                target_stg_cursor.close()
                target_stg_cursor = None
                target_stg_conn.close()
                target_stg_conn = None

                if stg_load_row_count > 0:
                    target_tx_conn = target_hook.get_conn()
                    target_tx_conn.autocommit = False
                    target_tx_cursor = target_tx_conn.cursor()

                    try:
                        if target_pre_sql:
                            target_tx_cursor.execute(target_pre_sql)
                            print(f"{target_table} target_pre_sql completed")

                        if load_option == "ui":
                            target_tx_cursor.execute(merge_ui_sql)
                            print(
                                f"{stg_table} -> {target_table} MERGE completed (UI), total={stg_load_row_count}"
                            )

                        elif load_option == "di":
                            target_tx_cursor.execute(delete_sql)
                            print(f"{target_table} DELETE completed")

                            target_tx_cursor.execute(insert_sql)
                            target_insert_count = stg_load_row_count
                            print(
                                f"{stg_table} -> {target_table} INSERT completed (DI), total={stg_load_row_count}"
                            )

                        elif load_option == "ti":
                            target_tx_cursor.execute(truncate_target_sql)
                            print(f"{target_table} TRUNCATE completed")

                            target_tx_cursor.execute(insert_sql)
                            target_insert_count = stg_load_row_count
                            print(
                                f"{stg_table} -> {target_table} INSERT completed (TI), total={stg_load_row_count}"
                            )

                        elif load_option == "i":
                            target_tx_cursor.execute(insert_sql)
                            target_insert_count = stg_load_row_count
                            print(
                                f"{stg_table} -> {target_table} INSERT completed (I), total={stg_load_row_count}"
                            )

                        elif load_option == "u":
                            if not merge_update_sql:
                                print(
                                    f"{target_table}: no non-pk columns to update, UPDATE skipped (U)"
                                )
                            else:
                                target_tx_cursor.execute(merge_update_sql)
                                print(f"{target_table} MERGE UPDATE completed (U)")

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

                if target_stg_cursor is not None:
                    target_stg_cursor.close()

                if target_stg_conn is not None:
                    target_stg_conn.close()

                if target_tx_cursor is not None:
                    target_tx_cursor.close()

                if target_tx_conn is not None:
                    target_tx_conn.close()

                if job_succeeded and stg_drop_yn == "Y":
                    try:
                        target_hook.run(drop_stg_sql)
                        print(f"{stg_table} dropped (stg_drop_yn=Y)")
                    except Exception:
                        print(f"{stg_table} drop failed after success")
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

        except Exception:
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
    run_etl.expand(table_config=table_configs)