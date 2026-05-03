from __future__ import annotations

import json
from datetime import datetime
from html import escape
from urllib.parse import urlencode

from flask import request, redirect
from flask_appbuilder import BaseView, expose
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.postgres.hooks.postgres import PostgresHook
from flask_wtf.csrf import generate_csrf


META_POSTGRES_CONN_ID = "postgres_conn"
META_SCHEMA = "public"

REQUIRED_COLUMNS = {
    "dag_id",
    "task_name",
}

SYSTEM_COLUMNS = {
    "id",
    "created_at",
    "updated_at",
    "update_dt",
}

COLUMN_COMMENTS = {
    "etl_meta_db_to_db": {
        "job_type": "dag 스케쥴 타입 입력 : daily, weekly, monthly, hourly, minute",
        "dag_id": "dag 이름 입력",
        "load_option": "ui : upsert, di: delete/insert, ti: truncate/insert, u: update only, i: insert only, d: delete only",
        "source_table": "소스 테이블 명 입력",
        "target_table": "타겟 테이블 명 입력",
        "pk_column": "타겟 pk 컬럼명 col1,col2.. 기술",
        "source_exec_sql": "source 추출 쿼리",
        "column_mapping": "일반 컬럼명 col1,col2.. 기술",
        "target_pre_sql": "STG 테이블 적재 완료 후 수행, 타겟 STG -> 본 테이블 적재 전 수행 sql, pre,post,본테이블 적재 에러 발생시 전부 rollback",
        "target_post_sql": "본 테이블 적재 완료 후 수행, 타겟 STG -> 본 테이블 적재 후 수행 sql, pre,post,본테이블 적재 에러 발생시 전부 rollback",
        "input_param": "JSON 형식으로 사용할 파라미터 기술, 입력한 파라미터 값 source_exec_sql 내부에 존재시 치환",
        "config_option": "JSON 형식으로 매핑 정보 추가 소스,타겟 타입 등 설정 정보",
        "create_dt": "생성 일자",
        "disable_dt": "비활성화 일자",
        "update_dt": "최종 수정 일자",
        "stg_drop_yn": "y 이면 임시테이블 드랍 n 이면 유지, y 여도 작업 에러시 무조건 남김",
        "enable_yn": "y 이면 데이터 처리 수행 n 이면 제외",
    },
    "etl_meta_file_to_db": {
        "job_type": "dag 스케쥴 타입 입력 : daily, weekly, monthly, hourly, minute",
        "dag_id": "dag 이름 입력",
        "load_option": "ui : upsert, di: delete/insert, ti: truncate/insert, u: update only, i: insert only, d: delete only",
        "source_table": "소스 파일 명 입력 : customer_$$p_base_dt_*.csv, customer_$$p_base_dt.csv",
        "target_table": "타겟 테이블 명 입력",
        "pk_column": "타겟 pk 컬럼명 col1,col2.. 기술",
        "column_mapping": "일반 컬럼명 col1,col2.. 기술",
        "source_file_type": "소스 파일 유형, json: 대괄호 중괄호 둘다 가능, csv: 파일에 헤더 포함 및 딜리미터 필요, text: 1 column",
        "csv_file_delimiter": "csv 파일 전용 딜리미터 지정 : , | \\t tab",
        "source_file_encoding": "소스 파일 인코딩 utf-8, euc-kr, cp949, ms949 입력",
        "source_file_dir": "소스 파일 경로",
        "source_pre_cmd": "소스 파일 읽기 전 전처리 로직 수행",
        "target_pre_sql": "STG 테이블 적재 완료 후 수행, 타겟 STG -> 본 테이블 적재 전 수행 sql, pre,post,본테이블 적재 에러 발생시 전부 rollback",
        "target_post_sql": "본 테이블 적재 완료 후 수행, 타겟 STG -> 본 테이블 적재 후 수행 sql, pre,post,본테이블 적재 에러 발생시 전부 rollback",
        "input_param": "JSON 형식으로 사용할 파라미터 기술, 입력한 파라미터 값 source_exec_sql 내부에 존재시 치환",
        "config_option": "JSON 형식으로 매핑 정보 추가 소스,타겟 타입 등 설정 정보",
        "create_dt": "생성 일자",
        "disable_dt": "비활성화 일자",
        "update_dt": "최종 수정 일자",
        "stg_drop_yn": "y 이면 임시테이블 드랍 n 이면 유지, y 여도 작업 에러시 무조건 남김",
        "enable_yn": "y 이면 데이터 처리 수행 n 이면 제외",
    },
    "etl_meta_db_to_file": {
        "job_type": "dag 스케쥴 타입 입력 : daily, weekly, monthly, hourly, minute",
        "dag_id": "dag 이름 입력",
        "source_table": "소스 테이블 명 입력",
        "target_table": "타겟 테이블 명 입력",
        "source_exec_sql": "source 추출 쿼리",
        "column_mapping": "일반 컬럼명 col1,col2.. 기술",
        "target_file_type": "타겟 파일 유형, json: 대괄호 중괄호 둘다 가능, csv: 파일에 헤더 포함 및 딜리미터 필요, text: 1 column",
        "csv_file_delimiter": "csv 파일 전용 딜리미터 지정 : , | \\t tab",
        "target_file_encoding": "타겟 파일 인코딩 utf-8, euc-kr, cp949, ms949 입력",
        "target_file_dir": "타겟 파일 경로",
        "target_pre_cmd": "타겟 파일 생성 전 전처리 로직 수행",
        "target_post_cmd": "타겟 파일 생성 후 후처리 로직 수행",
        "input_param": "JSON 형식으로 사용할 파라미터 기술, 입력한 파라미터 값 source_exec_sql 내부에 존재시 치환",
        "config_option": "JSON 형식으로 매핑 정보 추가 소스,타겟 타입 등 설정 정보",
        "create_dt": "생성 일자",
        "disable_dt": "비활성화 일자",
        "update_dt": "최종 수정 일자",
        "stg_drop_yn": "y 이면 임시테이블 드랍 n 이면 유지, y 여도 작업 에러시 무조건 남김",
        "enable_yn": "y 이면 데이터 처리 수행 n 이면 제외",
    },
}

META_TABLES = {
    "db_to_db": {
        "label": "DB → DB",
        "table": "etl_meta_db_to_db",
    },
    "db_to_file": {
        "label": "DB → File",
        "table": "etl_meta_db_to_file",
    },
    "file_to_db": {
        "label": "File → DB",
        "table": "etl_meta_file_to_db",
    },
}


class EtlMetaView(BaseView):
    route_base = "/etl-meta"
    default_view = "list"

    def _get_meta_type(self):
        meta_type = request.args.get("meta_type", "db_to_db")

        if meta_type not in META_TABLES:
            meta_type = "db_to_db"

        return meta_type

    def _get_table_name(self, meta_type):
        return META_TABLES[meta_type]["table"]

    def _get_columns(self, table_name):
        hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)

        sql = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """

        rows = hook.get_records(sql, parameters=(META_SCHEMA, table_name))

        if not rows:
            raise Exception(f"Table not found or no columns: {META_SCHEMA}.{table_name}")

        return [
            {
                "name": r[0],
                "data_type": r[1],
                "is_nullable": r[2],
                "column_default": r[3],
            }
            for r in rows
        ]

    def _get_insert_columns(self, table_name):
        columns = self._get_columns(table_name)

        insert_columns = []

        for col in columns:
            name = col["name"]
            default = col["column_default"]

            if name in SYSTEM_COLUMNS:
                continue

            if default and "nextval" in str(default).lower():
                continue

            insert_columns.append(col)

        return insert_columns

    def _get_editable_columns(self, columns):
        editable_columns = []

        for col in columns:
            name = col["name"]
            default = col["column_default"]

            if name in SYSTEM_COLUMNS:
                continue

            if default and "nextval" in str(default).lower():
                continue

            editable_columns.append(col)

        return editable_columns

    def _build_row_key_sql(self, column_names):
        """
        화면 ROW 식별용 key.
        id 컬럼이 있으면 id 기준, 없으면 PostgreSQL ctid 기준.
        """
        if "id" in column_names:
            return '"id"::text AS __row_key', '"id"::text'

        return "ctid::text AS __row_key", "ctid::text"

    def _get_selected_rows(self):
        return request.form.getlist("selected_rows")

    def _convert_column_mapping_1_to_1(self, raw_value: str | None) -> str:
        if raw_value is None:
            return ""

        raw_value = raw_value.strip()
        if not raw_value:
            return ""

        result = {}
        for col in raw_value.split(","):
            col = col.strip()
            if not col:
                continue

            upper_col = col.upper()
            result[upper_col] = upper_col

        if not result:
            return ""

        return json.dumps(result, ensure_ascii=False, separators=(",", ":"))

    def _parse_sort_spec(self, raw_sort: str | None, column_names: list[str]) -> list[tuple[str, str]]:
        if not raw_sort:
            return []

        result = []

        for item in raw_sort.split(","):
            item = item.strip()
            if not item:
                continue

            if ":" in item:
                col, direction = item.split(":", 1)
            else:
                col, direction = item, "asc"

            col = col.strip()
            direction = direction.strip().lower()

            if col not in column_names:
                continue

            if direction not in ("asc", "desc"):
                direction = "asc"

            result.append((col, direction))

        return result

    def _build_order_sql(self, sort_specs: list[tuple[str, str]], column_names: list[str]) -> str:
        if sort_specs:
            order_items = []
            for col, direction in sort_specs:
                order_items.append(f'"{col}" {direction.upper()}')
            return "ORDER BY " + ", ".join(order_items)

        for candidate in ["created_at", "updated_at", "update_dt", "id", "dag_id", "source_table", "target_table"]:
            if candidate in column_names:
                return f'ORDER BY "{candidate}" DESC'

        return ""

    def _build_sort_query_string(self, meta_type, search_column, search_text, current_sort, clicked_col):
        sort_specs = []

        if current_sort:
            for item in current_sort.split(","):
                item = item.strip()
                if not item:
                    continue

                if ":" in item:
                    col, direction = item.split(":", 1)
                else:
                    col, direction = item, "asc"

                col = col.strip()
                direction = direction.strip().lower()

                if col:
                    sort_specs.append((col, direction))

        found = False
        new_specs = []

        for col, direction in sort_specs:
            if col == clicked_col:
                found = True

                if direction == "asc":
                    new_specs.append((col, "desc"))
                elif direction == "desc":
                    # DESC 상태에서 한 번 더 클릭하면 정렬 조건에서 제거
                    continue
                else:
                    new_specs.append((col, "asc"))
            else:
                new_specs.append((col, direction))

        if not found:
            new_specs.append((clicked_col, "asc"))

        new_sort = ",".join([f"{col}:{direction}" for col, direction in new_specs])

        query_params = {
            "meta_type": meta_type,
        }

        if search_column:
            query_params["search_column"] = search_column

        if search_text:
            query_params["search_text"] = search_text

        if new_sort:
            query_params["sort"] = new_sort

        return urlencode(query_params)

    def _get_sort_mark(self, sort_specs: list[tuple[str, str]], col_name: str) -> str:
        for idx, (col, direction) in enumerate(sort_specs, start=1):
            if col == col_name:
                arrow = "▲" if direction == "asc" else "▼"
                return f" {arrow}{idx}"

        return ""

    def _build_sort_header(self, meta_type, column_name, sort_specs, current_sort, search_column, search_text):
        query_string = self._build_sort_query_string(
            meta_type=meta_type,
            search_column=search_column,
            search_text=search_text,
            current_sort=current_sort,
            clicked_col=column_name,
        )

        sort_mark = self._get_sort_mark(sort_specs, column_name)

        return f'''
        <th>
            <a class="sort-link" href="/etl-meta/?{escape(query_string, quote=True)}">
                {escape(column_name)}{escape(sort_mark)}
            </a>
        </th>
        '''

    def _convert_value(self, value, data_type):
        if value is None:
            return None

        value = value.strip()

        if value == "":
            return None

        if data_type in ("integer", "bigint", "smallint"):
            return int(value)

        if data_type in ("numeric", "double precision", "real"):
            return float(value)

        if data_type == "boolean":
            return value.lower() in ("true", "1", "y", "yes", "on")

        if data_type in ("json", "jsonb"):
            return json.dumps(json.loads(value), ensure_ascii=False)

        if data_type in ("timestamp without time zone", "timestamp with time zone"):
            return datetime.fromisoformat(value)

        return value

    def _render_top_buttons(self):
        return """
        <div class="top-buttons">
            <a class="btn gray" href="/home">Airflow 콘솔로 돌아가기</a>
            <a class="btn gray" href="/etl-meta/">ETL Meta 관리</a>
            <a class="btn" href="/etl-meta/flow">ETL Flow 보기</a>
        </div>
        """

    def _render_tabs(self, selected_meta_type):
        html = ""

        for meta_type, info in META_TABLES.items():
            label = info["label"]
            active_class = "active" if meta_type == selected_meta_type else ""

            html += f"""
            <a class="tab {active_class}" href="/etl-meta/?meta_type={escape(meta_type)}">
                {escape(label)}
            </a>
            """

        return html

    def _build_search_area(self, meta_type, column_names, search_column, search_text):
        option_html = ""

        for col in column_names:
            selected = "selected" if col == search_column else ""
            option_html += f"""
            <option value="{escape(col)}" {selected}>{escape(col)}</option>
            """

        return f"""
        <form class="search-box" method="get" action="/etl-meta/">
            <input type="hidden" name="meta_type" value="{escape(meta_type)}">

            <div class="search-row">
                <div class="search-item">
                    <label>항목</label>
                    <select name="search_column">
                        <option value="">전체</option>
                        {option_html}
                    </select>
                </div>

                <div class="search-keyword">
                    <label>검색</label>
                    <input
                        type="text"
                        name="search_text"
                        value="{escape(search_text)}"
                        placeholder="예: customer 또는 =customer"
                    >
                </div>

                <div class="search-button">
                    <label>&nbsp;</label>
                    <button class="btn" type="submit">검색</button>
                    <a class="btn gray" href="/etl-meta/?meta_type={escape(meta_type)}">초기화</a>
                </div>
            </div>

            <div class="search-help">
                일반 검색: <b>LIKE</b> 검색 / 검색어 앞에 <b>=</b> 입력 시 정확히 일치 검색
            </div>
        </form>
        """

    def _build_search_sql(self, column_names, search_column, search_text):
        if not search_text:
            return "", []

        search_text = search_text.strip()

        if not search_text:
            return "", []

        is_equal_search = search_text.startswith("=")

        if is_equal_search:
            keyword = search_text[1:].strip()
            operator = "="
            value = keyword
        else:
            keyword = search_text
            operator = "LIKE"
            value = f"%{keyword}%"

        if not keyword:
            return "", []

        if search_column and search_column in column_names:
            where_sql = f'WHERE "{search_column}"::text {operator} %s'
            return where_sql, [value]

        conditions = []

        for col in column_names:
            conditions.append(f'"{col}"::text {operator} %s')

        where_sql = "WHERE " + " OR ".join(conditions)
        params = [value] * len(column_names)

        return where_sql, params

    def _validate_required_values(self, form_values):
        for required_col in REQUIRED_COLUMNS:
            if not form_values.get(required_col, "").strip():
                return False

        return True

    def _short_text(self, value, max_len: int = 80) -> str:
        if value is None:
            return "-"

        text = str(value).strip()
        if not text:
            return "-"

        text = " ".join(text.split())

        if len(text) > max_len:
            return text[:max_len] + "..."

        return text

    def _mermaid_escape(self, value) -> str:
        text = self._short_text(value)
        return (
            text.replace("\\", "\\\\")
            .replace('"', "'")
            .replace("[", "(")
            .replace("]", ")")
            .replace("{", "(")
            .replace("}", ")")
            .replace("|", "/")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    def _get_dag_ids(self, table_name: str) -> list[str]:
        columns = self._get_columns(table_name)
        column_names = [c["name"] for c in columns]

        if "dag_id" not in column_names:
            return []

        hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)
        rows = hook.get_records(
            f'''
            SELECT DISTINCT dag_id
            FROM {META_SCHEMA}.{table_name}
            WHERE dag_id IS NOT NULL
              AND TRIM(dag_id) <> ''
            ORDER BY dag_id
            '''
        )

        return [str(r[0]) for r in rows if r and r[0] is not None]

    def _get_flow_rows(self, table_name: str, dag_id: str) -> list[dict]:
        columns = self._get_columns(table_name)
        column_names = [c["name"] for c in columns]
        column_set = set(column_names)

        flow_columns = [
            "task_name",
            "exec_seq",
            "source_table",
            "target_table",
            "load_option",
            "enable_yn",
            "target_pre_sql",
            "target_post_sql",
            "target_pre_cmd",
            "target_post_cmd",
            "source_pre_cmd",
        ]

        select_items = []
        for col in flow_columns:
            if col in column_set:
                select_items.append(f'"{col}"')
            else:
                select_items.append(f"NULL AS {col}")

        if "dag_id" not in column_set:
            return []

        order_items = []
        if "exec_seq" in column_set:
            order_items.append("COALESCE(exec_seq, 999999)")
        if "task_name" in column_set:
            order_items.append("task_name")
        if "source_table" in column_set:
            order_items.append("source_table")
        if "target_table" in column_set:
            order_items.append("target_table")

        order_sql = "ORDER BY " + ", ".join(order_items) if order_items else ""

        hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)
        records = hook.get_records(
            f'''
            SELECT {", ".join(select_items)}
            FROM {META_SCHEMA}.{table_name}
            WHERE dag_id = %s
            {order_sql}
            ''',
            parameters=(dag_id,),
        )

        rows = []
        for record in records:
            rows.append(dict(zip(flow_columns, record)))

        return rows

    def _build_flow_type_buttons(self, selected_meta_type: str, selected_dag_id: str | None = None) -> str:
        html = ""

        for meta_type, info in META_TABLES.items():
            active_class = "active" if meta_type == selected_meta_type else ""
            query_params = {"meta_type": meta_type}
            if selected_dag_id and meta_type == selected_meta_type:
                query_params["dag_id"] = selected_dag_id

            html += f'''
            <a class="tab {active_class}" href="/etl-meta/flow?{escape(urlencode(query_params), quote=True)}">
                {escape(info["label"])}
            </a>
            '''

        return html

    def _build_flow_dag_select(self, meta_type: str, dag_ids: list[str], selected_dag_id: str) -> str:
        option_html = ""

        for dag_id in dag_ids:
            selected = "selected" if dag_id == selected_dag_id else ""
            option_html += f'''
            <option value="{escape(dag_id, quote=True)}" {selected}>{escape(dag_id)}</option>
            '''

        return f'''
        <form class="search-box" method="get" action="/etl-meta/flow">
            <input type="hidden" name="meta_type" value="{escape(meta_type, quote=True)}">
            <div class="search-row">
                <div class="search-keyword">
                    <label>dag_id</label>
                    <select name="dag_id">
                        {option_html}
                    </select>
                </div>
                <div class="search-button">
                    <label>&nbsp;</label>
                    <button class="btn" type="submit">FLOW 조회</button>
                </div>
            </div>
        </form>
        '''

    def _build_mermaid_flow(self, flow_rows: list[dict]) -> str:
        if not flow_rows:
            return 'flowchart LR\n    EMPTY["조회된 TASK가 없습니다"]'

        lines = ["flowchart LR"]
        lines.append("    classDef active fill:#e8f4ff,stroke:#017cee,stroke-width:1px,color:#111;")
        lines.append("    classDef disabled fill:#eeeeee,stroke:#999,stroke-width:1px,color:#777;")

        for idx, row in enumerate(flow_rows):
            node_id = f"T{idx}"
            enable_yn = self._short_text(row.get("enable_yn"), 10).upper()
            node_class = "disabled" if enable_yn == "N" else "active"

            label_items = [
                ("task_name", row.get("task_name")),
                ("exec_seq", row.get("exec_seq")),
                ("source_table", row.get("source_table")),
                ("target_table", row.get("target_table")),
                ("load_option", row.get("load_option")),
                ("enable_yn", row.get("enable_yn")),
                ("target_pre_sql", row.get("target_pre_sql")),
                ("target_post_sql", row.get("target_post_sql")),
                ("target_pre_cmd", row.get("target_pre_cmd")),
                ("target_post_cmd", row.get("target_post_cmd")),
                ("source_pre_cmd", row.get("source_pre_cmd")),
            ]

            label = "<br/>".join(
                [f"{escape(k)}: {self._mermaid_escape(v)}" for k, v in label_items]
            )

            lines.append(f'    {node_id}["{label}"]:::{node_class}')

        for idx in range(len(flow_rows) - 1):
            lines.append(f"    T{idx} --> T{idx + 1}")

        return "\n".join(lines)

    @expose("/flow", methods=["GET"])
    def flow(self):
        meta_type = self._get_meta_type()
        table_name = self._get_table_name(meta_type)
        selected_dag_id = request.args.get("dag_id", "").strip()

        dag_ids = self._get_dag_ids(table_name)

        if not selected_dag_id and dag_ids:
            selected_dag_id = dag_ids[0]

        if selected_dag_id and selected_dag_id not in dag_ids:
            selected_dag_id = dag_ids[0] if dag_ids else ""

        flow_rows = self._get_flow_rows(table_name, selected_dag_id) if selected_dag_id else []
        mermaid_text = self._build_mermaid_flow(flow_rows)

        top_buttons_html = self._render_top_buttons()
        flow_type_buttons_html = self._build_flow_type_buttons(meta_type, selected_dag_id)
        dag_select_html = self._build_flow_dag_select(meta_type, dag_ids, selected_dag_id) if dag_ids else ""

        no_dag_message = ""
        if not dag_ids:
            no_dag_message = f'''
            <div class="error-message">
                {escape(META_SCHEMA)}.{escape(table_name)} 테이블에 조회 가능한 dag_id가 없습니다.
            </div>
            '''

        html = f'''
        <html>
        <head>
            <style>
                .page {{
                    padding: 20px;
                }}

                .top-buttons {{
                    margin-bottom: 15px;
                }}

                .tabs {{
                    margin-bottom: 20px;
                }}

                .tab {{
                    display: inline-block;
                    padding: 9px 14px;
                    margin-right: 6px;
                    background-color: #333;
                    color: white;
                    text-decoration: none;
                    border-radius: 4px;
                }}

                .tab.active {{
                    background-color: #017cee;
                }}

                .btn {{
                    display: inline-block;
                    padding: 8px 14px;
                    background-color: #017cee;
                    color: white;
                    text-decoration: none;
                    border-radius: 4px;
                    margin-bottom: 15px;
                    border: none;
                    cursor: pointer;
                }}

                .btn.gray {{
                    background-color: #666;
                }}

                .search-box {{
                    padding: 15px;
                    margin-bottom: 20px;
                    border: 1px solid #333;
                    border-radius: 6px;
                    background-color: #111;
                }}

                .search-row {{
                    display: flex;
                    gap: 12px;
                    align-items: end;
                }}

                .search-keyword {{
                    width: 420px;
                }}

                .search-button {{
                    min-width: 180px;
                }}

                .search-box label {{
                    display: block;
                    margin-bottom: 5px;
                    font-weight: bold;
                    color: white;
                }}

                .search-box select {{
                    width: 100%;
                    padding: 8px;
                    border: 1px solid #555;
                    border-radius: 4px;
                    background-color: #222;
                    color: white;
                }}

                .flow-panel {{
                    padding: 16px;
                    border: 1px solid #ddd;
                    border-radius: 8px;
                    background-color: white;
                    overflow-x: auto;
                }}

                .flow-help {{
                    margin-bottom: 10px;
                    color: #666;
                    font-size: 12px;
                }}

                .mermaid {{
                    min-width: 900px;
                }}

                .error-message {{
                    margin-bottom: 15px;
                    padding: 12px 14px;
                    background-color: #ffe8e8;
                    border: 1px solid #ff9f9f;
                    color: #b00020;
                    border-radius: 4px;
                    font-weight: bold;
                }}

                pre.flow-source {{
                    margin-top: 16px;
                    padding: 12px;
                    background-color: #f7f7f7;
                    border: 1px solid #ddd;
                    border-radius: 6px;
                    white-space: pre-wrap;
                    font-size: 12px;
                }}
            </style>
            <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
            <script>
                if (window.mermaid) {{
                    mermaid.initialize({{
                        startOnLoad: true,
                        securityLevel: 'loose',
                        theme: 'default',
                        flowchart: {{
                            htmlLabels: true,
                            curve: 'basis'
                        }}
                    }});
                }}
            </script>
        </head>
        <body>
            <div class="page">
                {top_buttons_html}

                <h2>ETL Flow</h2>

                <div class="tabs">
                    {flow_type_buttons_html}
                </div>

                <h3>{escape(META_SCHEMA)}.{escape(table_name)}</h3>

                {dag_select_html}
                {no_dag_message}

                <div class="flow-help">
                    선택한 FLOW 유형과 dag_id 기준으로 exec_seq 오름차순 순차 FLOW를 표시합니다.
                    enable_yn = N 인 TASK는 회색으로 표시됩니다.
                </div>

                <div class="flow-panel">
                    <div class="mermaid">
{escape(mermaid_text)}
                    </div>
                </div>

                <details>
                    <summary>Mermaid source 보기</summary>
                    <pre class="flow-source">{escape(mermaid_text)}</pre>
                </details>
            </div>
        </body>
        </html>
        '''

        return html

    @expose("/delete", methods=["POST"])
    def delete(self):
        meta_type = self._get_meta_type()
        table_name = self._get_table_name(meta_type)

        selected_rows = self._get_selected_rows()

        if not selected_rows:
            return redirect(f"/etl-meta/?meta_type={meta_type}")

        columns = self._get_columns(table_name)
        column_names = [c["name"] for c in columns]
        _, row_key_where_sql = self._build_row_key_sql(column_names)

        placeholders = ", ".join(["%s"] * len(selected_rows))

        delete_sql = f"""
        DELETE FROM {META_SCHEMA}.{table_name}
        WHERE {row_key_where_sql} IN ({placeholders})
        """

        hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)
        hook.run(delete_sql, parameters=tuple(selected_rows))

        return redirect(f"/etl-meta/?meta_type={meta_type}")

    @expose("/disable", methods=["POST"])
    def disable(self):
        meta_type = self._get_meta_type()
        table_name = self._get_table_name(meta_type)

        selected_rows = self._get_selected_rows()

        if not selected_rows:
            return redirect(f"/etl-meta/?meta_type={meta_type}")

        columns = self._get_columns(table_name)
        column_names = [c["name"] for c in columns]

        if "enable_yn" not in column_names:
            raise Exception(f"{META_SCHEMA}.{table_name}: enable_yn column does not exist")

        _, row_key_where_sql = self._build_row_key_sql(column_names)

        set_sql_list = ['"enable_yn" = \'N\'']

        if "disable_dt" in column_names:
            set_sql_list.append('"disable_dt" = TO_CHAR(NOW(), \'YYYYMMDD\')')

        if "update_dt" in column_names:
            set_sql_list.append('"update_dt" = TO_CHAR(NOW(), \'YYYYMMDD\')')

        set_sql = ", ".join(set_sql_list)
        placeholders = ", ".join(["%s"] * len(selected_rows))

        update_sql = f"""
        UPDATE {META_SCHEMA}.{table_name}
           SET {set_sql}
         WHERE {row_key_where_sql} IN ({placeholders})
        """

        hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)
        hook.run(update_sql, parameters=tuple(selected_rows))

        return redirect(f"/etl-meta/?meta_type={meta_type}")

    @expose("/save", methods=["POST"])
    def save(self):
        meta_type = self._get_meta_type()
        table_name = self._get_table_name(meta_type)

        columns = self._get_columns(table_name)
        column_names = [c["name"] for c in columns]
        editable_columns = self._get_editable_columns(columns)
        _, row_key_where_sql = self._build_row_key_sql(column_names)

        row_keys = request.form.getlist("row_keys")

        if not row_keys:
            return redirect(f"/etl-meta/?meta_type={meta_type}")

        hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)

        conn = None
        cursor = None

        try:
            conn = hook.get_conn()
            conn.autocommit = False
            cursor = conn.cursor()

            for row_idx, row_key in enumerate(row_keys):
                row_changed = request.form.get(f"row_changed_{row_idx}", "N")

                if row_changed != "Y":
                    continue

                set_cols = []
                values = []

                for col in editable_columns:
                    col_name = col["name"]
                    data_type = col["data_type"]
                    form_key = f"cell_{row_idx}_{col_name}"

                    if form_key not in request.form:
                        continue

                    raw_value = request.form.get(form_key)
                    converted_value = self._convert_value(raw_value, data_type)

                    set_cols.append(f'"{col_name}" = %s')
                    values.append(converted_value)

                if "update_dt" in column_names:
                    set_cols.append('"update_dt" = TO_CHAR(NOW(), \'YYYYMMDD\')')

                if not set_cols:
                    continue

                values.append(row_key)

                update_sql = f"""
                UPDATE {META_SCHEMA}.{table_name}
                   SET {", ".join(set_cols)}
                 WHERE {row_key_where_sql} = %s
                """

                cursor.execute(update_sql, tuple(values))

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

        return redirect(f"/etl-meta/?meta_type={meta_type}")

    @expose("/")
    def list(self):
        meta_type = self._get_meta_type()
        table_name = self._get_table_name(meta_type)

        search_column = request.args.get("search_column", "")
        search_text = request.args.get("search_text", "")
        sort = request.args.get("sort", "")

        hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)

        columns = self._get_columns(table_name)
        column_names = [c["name"] for c in columns]

        if search_column not in column_names:
            search_column = ""

        sort_specs = self._parse_sort_spec(sort, column_names)

        select_cols = ", ".join([f'"{c}"' for c in column_names])
        row_key_select_sql, _ = self._build_row_key_sql(column_names)

        where_sql, search_params = self._build_search_sql(
            column_names=column_names,
            search_column=search_column,
            search_text=search_text,
        )

        order_sql = self._build_order_sql(sort_specs, column_names)

        rows = hook.get_records(
            f"""
            SELECT
                {row_key_select_sql},
                {select_cols}
            FROM {META_SCHEMA}.{table_name}
            {where_sql}
            {order_sql}
            LIMIT 100
            """,
            parameters=tuple(search_params),
        )

        tabs_html = self._render_tabs(meta_type)
        top_buttons_html = self._render_top_buttons()

        search_area_html = self._build_search_area(
            meta_type=meta_type,
            column_names=column_names,
            search_column=search_column,
            search_text=search_text,
        )

        editable_column_names = {
            col["name"]
            for col in self._get_editable_columns(columns)
        }

        header_html = """
        <th style="width: 40px; text-align: center;">
            <input type="checkbox" onclick="toggleAllRows(this)">
        </th>
        """

        header_html += "".join(
            self._build_sort_header(
                meta_type=meta_type,
                column_name=col,
                sort_specs=sort_specs,
                current_sort=sort,
                search_column=search_column,
                search_text=search_text,
            )
            for col in column_names
        )

        row_html = ""

        for row_idx, row in enumerate(rows):
            row_key = row[0]
            row_values = row[1:]

            row_html += "<tr>"
            row_html += f"""
            <td style="text-align: center;">
                <input type="checkbox" name="selected_rows" value="{escape(str(row_key))}">
                <input type="hidden" name="row_keys" value="{escape(str(row_key))}">
                <input type="hidden" name="row_changed_{row_idx}" id="row_changed_{row_idx}" value="N">
            </td>
            """

            for col_idx, value in enumerate(row_values):
                col_name = column_names[col_idx]
                value_text = "" if value is None else str(value)
                value_attr = escape(value_text, quote=True)

                if col_name in editable_column_names:
                    row_html += f"""
                    <td class="editable-cell" ondblclick="startEdit(this)">
                        <span class="cell-view">{escape(value_text)}</span>
                        <input
                            class="cell-input"
                            type="text"
                            name="cell_{row_idx}_{escape(col_name, quote=True)}"
                            value="{value_attr}"
                            data-original="{value_attr}"
                            data-row-index="{row_idx}"
                            onblur="finishEdit(this)"
                            onkeydown="handleCellKeydown(event, this)"
                        >
                    </td>
                    """
                else:
                    row_html += f"""
                    <td>{escape(value_text)}</td>
                    """

            row_html += "</tr>"

        csrf_token = generate_csrf()

        html = f"""
        <html>
        <head>
            <style>
                .page {{
                    padding: 20px;
                }}

                .top-buttons {{
                    margin-bottom: 15px;
                }}

                .tabs {{
                    margin-bottom: 20px;
                }}

                .tab {{
                    display: inline-block;
                    padding: 9px 14px;
                    margin-right: 6px;
                    background-color: #333;
                    color: white;
                    text-decoration: none;
                    border-radius: 4px;
                }}

                .tab.active {{
                    background-color: #017cee;
                }}

                .btn {{
                    display: inline-block;
                    padding: 8px 14px;
                    background-color: #017cee;
                    color: white;
                    text-decoration: none;
                    border-radius: 4px;
                    margin-bottom: 15px;
                    border: none;
                    cursor: pointer;
                }}

                .btn.gray {{
                    background-color: #666;
                }}

                .btn.warning {{
                    background-color: #faad14;
                    color: #111;
                }}

                .btn.danger {{
                    background-color: #d9363e;
                }}

                .search-box {{
                    padding: 15px;
                    margin-bottom: 20px;
                    border: 1px solid #333;
                    border-radius: 6px;
                    background-color: #111;
                }}

                .search-row {{
                    display: flex;
                    gap: 12px;
                    align-items: end;
                }}

                .search-item {{
                    width: 260px;
                }}

                .search-keyword {{
                    width: 420px;
                }}

                .search-button {{
                    min-width: 180px;
                }}

                .search-box label {{
                    display: block;
                    margin-bottom: 5px;
                    font-weight: bold;
                }}

                .search-box input,
                .search-box select {{
                    width: 100%;
                    padding: 8px;
                    border: 1px solid #555;
                    border-radius: 4px;
                    background-color: #222;
                    color: white;
                }}

                .search-help {{
                    margin-top: 8px;
                    color: #aaa;
                    font-size: 12px;
                }}

                .table-actions {{
                    margin-bottom: 10px;
                }}

                .edit-help {{
                    margin-bottom: 10px;
                    color: #888;
                    font-size: 12px;
                }}

                table {{
                    border-collapse: collapse;
                    width: 100%;
                    font-size: 13px;
                }}

                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                    vertical-align: top;
                }}

                th {{
                    background-color: #f5f5f5;
                    color: #222;
                }}

                .editable-cell {{
                    cursor: pointer;
                }}

                .editable-cell:hover {{
                    background-color: #fffbe6;
                }}

                .cell-input {{
                    display: none;
                    width: 100%;
                    min-width: 120px;
                    padding: 5px;
                    border: 1px solid #017cee;
                    border-radius: 3px;
                    font-size: 13px;
                }}

                .cell-view {{
                    display: inline-block;
                    min-width: 20px;
                    white-space: pre-wrap;
                }}
            </style>
        </head>
        <body>
            <div class="page">
                {top_buttons_html}

                <h2>ETL Meta 관리</h2>

                <div class="tabs">
                    {tabs_html}
                </div>

                <h3>{escape(META_SCHEMA)}.{escape(table_name)}</h3>

                {search_area_html}

                <a class="btn" href="/etl-meta/new?meta_type={escape(meta_type)}">
                    신규 DAG 설정 등록
                </a>
                <a class="btn gray" href="/etl-meta/flow?meta_type={escape(meta_type)}">
                    FLOW 보기
                </a>

                <h3>최근 등록된 설정</h3>

                <form method="post" id="metaTableForm">
                    <input type="hidden" name="csrf_token" value="{csrf_token}">

                    <div class="table-actions">
                        <button
                            class="btn"
                            type="submit"
                            formaction="/etl-meta/save?meta_type={escape(meta_type)}"
                            onclick="return confirm('변경한 내용을 저장하시겠습니까?');"
                        >
                            저장
                        </button>

                        <button
                            class="btn warning"
                            type="submit"
                            formaction="/etl-meta/disable?meta_type={escape(meta_type)}"
                            onclick="return confirm('선택한 ROW를 비활성화 하시겠습니까?');"
                        >
                            선택 ROW 비활성화
                        </button>

                        <button
                            class="btn danger"
                            type="submit"
                            formaction="/etl-meta/delete?meta_type={escape(meta_type)}"
                            onclick="return confirm('선택한 ROW를 삭제하시겠습니까? 삭제 후 복구할 수 없습니다.');"
                        >
                            선택 ROW 삭제
                        </button>
                    </div>

                    <div class="edit-help">
                        컬럼 값을 더블 클릭하면 수정할 수 있습니다. 수정 후 저장 버튼을 누르면 변경됩니다.
                    </div>

                    <table>
                        <thead>
                            <tr>{header_html}</tr>
                        </thead>
                        <tbody>
                            {row_html}
                        </tbody>
                    </table>
                </form>
            </div>

            <script>
            function toggleAllRows(source) {{
                const checkboxes = document.getElementsByName("selected_rows");
                for (let i = 0; i < checkboxes.length; i++) {{
                    checkboxes[i].checked = source.checked;
                }}
            }}

            function startEdit(td) {{
                const view = td.querySelector(".cell-view");
                const input = td.querySelector(".cell-input");

                if (!view || !input) {{
                    return;
                }}

                view.style.display = "none";
                input.style.display = "block";
                input.focus();
                input.select();
            }}

            function finishEdit(input) {{
                const td = input.closest("td");
                const view = td.querySelector(".cell-view");
                const rowIndex = input.dataset.rowIndex;
                const changedInput = document.getElementById("row_changed_" + rowIndex);

                view.textContent = input.value;
                view.style.display = "inline-block";
                input.style.display = "none";

                if (input.value !== input.dataset.original && changedInput) {{
                    changedInput.value = "Y";
                    td.classList.add("edited-cell");
                }}
            }}

            function handleCellKeydown(event, input) {{
                if (event.key === "Enter") {{
                    event.preventDefault();
                    finishEdit(input);
                }}

                if (event.key === "Escape") {{
                    event.preventDefault();
                    input.value = input.dataset.original;
                    finishEdit(input);
                }}
            }}
            </script>
        </body>
        </html>
        """

        return html

    @expose("/new", methods=["GET", "POST"])
    def new(self):
        meta_type = self._get_meta_type()
        table_name = self._get_table_name(meta_type)

        insert_columns = self._get_insert_columns(table_name)
        form_values = {}
        error_message = ""
        column_mapping_mode = request.form.get("column_mapping_mode", "CUSTOM").strip().upper()

        if column_mapping_mode not in ("1:1", "CUSTOM"):
            column_mapping_mode = "CUSTOM"

        if request.method == "POST":
            for col in insert_columns:
                name = col["name"]
                form_values[name] = request.form.get(name, "")

            if not self._validate_required_values(form_values):
                error_message = "필수 항목이 입력 되지 않았습니다."
            else:
                if "column_mapping" in form_values and column_mapping_mode == "1:1":
                    form_values["column_mapping"] = self._convert_column_mapping_1_to_1(
                        form_values.get("column_mapping")
                    )

                hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)

                col_names = []
                values = []

                for col in insert_columns:
                    name = col["name"]
                    data_type = col["data_type"]

                    raw_value = form_values.get(name)
                    converted_value = self._convert_value(raw_value, data_type)

                    if converted_value is not None:
                        col_names.append(name)
                        values.append(converted_value)

                if not col_names:
                    error_message = "입력된 값이 없습니다."
                else:
                    placeholders = ", ".join(["%s"] * len(col_names))
                    columns_sql = ", ".join([f'"{c}"' for c in col_names])

                    insert_sql = f"""
                    INSERT INTO {META_SCHEMA}.{table_name}
                    ({columns_sql})
                    VALUES ({placeholders})
                    """

                    hook.run(insert_sql, parameters=tuple(values))

                    return redirect(f"/etl-meta/?meta_type={meta_type}")

        form_html = ""

        table_comments = COLUMN_COMMENTS.get(table_name, {})

        for col in insert_columns:
            name = col["name"]
            data_type = col["data_type"]
            nullable = col["is_nullable"]
            value = form_values.get(name, "")

            required_mark = '<span class="required">*</span>' if name in REQUIRED_COLUMNS else ""
            required = "required" if name in REQUIRED_COLUMNS else ""
            comment = table_comments.get(name, "")

            if name == "column_mapping":
                one_to_one_selected = "selected" if column_mapping_mode == "1:1" else ""
                custom_selected = "selected" if column_mapping_mode == "CUSTOM" else ""

                input_html = f"""
                <div class="column-mapping-option-row">
                    <select class="column-mapping-mode" name="column_mapping_mode">
                        <option value="1:1" {one_to_one_selected}>1:1</option>
                        <option value="CUSTOM" {custom_selected}>CUSTOM</option>
                    </select>
                    <span class="column-mapping-help">**custom 선택 시, {{"col1":"col1", ~~}} 형태로 입력</span>
                </div>
                <textarea name="{escape(name)}" rows="5" {required}>{escape(value)}</textarea>
                """
            elif data_type in ("json", "jsonb"):
                input_html = f"""
                <textarea name="{escape(name)}" rows="5" {required}>{escape(value)}</textarea>
                """
            elif data_type == "boolean":
                true_selected = "selected" if value.lower() == "true" else ""
                false_selected = "selected" if value.lower() == "false" else ""

                input_html = f"""
                <select name="{escape(name)}">
                    <option value=""></option>
                    <option value="true" {true_selected}>true</option>
                    <option value="false" {false_selected}>false</option>
                </select>
                """
            else:
                input_html = f"""
                <input type="text" name="{escape(name)}" value="{escape(value)}" {required}>
                """

            comment_html = ""
            if comment:
                comment_html = f"""
                <div class="comment">{escape(comment)}</div>
                """

            form_html += f"""
            <div class="form-row">
                <label>
                    {required_mark}
                    {escape(name)}
                    <span class="type">({escape(data_type)})</span>
                </label>
                {comment_html}
                {input_html}
            </div>
            """

        tabs_html = self._render_tabs(meta_type)
        top_buttons_html = self._render_top_buttons()

        error_html = ""
        if error_message:
            error_html = f"""
            <div class="error-message">
                {escape(error_message)}
            </div>
            """

        csrf_token = generate_csrf()

        html = f"""
        <html>
        <head>
            <style>
                .page {{
                    padding: 20px;
                    max-width: 950px;
                }}

                .top-buttons {{
                    margin-bottom: 15px;
                }}

                .tabs {{
                    margin-bottom: 20px;
                }}

                .tab {{
                    display: inline-block;
                    padding: 9px 14px;
                    margin-right: 6px;
                    background-color: #333;
                    color: white;
                    text-decoration: none;
                    border-radius: 4px;
                }}

                .tab.active {{
                    background-color: #017cee;
                }}

                .form-row {{
                    margin-bottom: 16px;
                    padding-bottom: 12px;
                    border-bottom: 1px solid #eee;
                }}

                label {{
                    display: block;
                    font-weight: bold;
                    margin-bottom: 5px;
                }}

                .required {{
                    color: #ff4d4f;
                    font-weight: bold;
                    margin-right: 4px;
                }}

                .type {{
                    color: #777;
                    font-weight: normal;
                    font-size: 12px;
                }}

                .comment {{
                    margin-bottom: 6px;
                    padding: 7px 9px;
                    background-color: #f5f5f5;
                    border-left: 4px solid #017cee;
                    color: #333;
                    font-size: 12px;
                    line-height: 1.4;
                }}

                input, textarea, select {{
                    width: 100%;
                    padding: 8px;
                    border: 1px solid #ccc;
                    border-radius: 4px;
                    font-size: 13px;
                }}

                .btn {{
                    display: inline-block;
                    padding: 9px 16px;
                    background-color: #017cee;
                    color: white;
                    border: none;
                    border-radius: 4px;
                    cursor: pointer;
                    text-decoration: none;
                }}

                .btn.gray {{
                    background-color: #666;
                }}

                .back {{
                    display: inline-block;
                    margin-bottom: 15px;
                }}

                .error-message {{
                    margin-bottom: 15px;
                    padding: 12px 14px;
                    background-color: #ffe8e8;
                    border: 1px solid #ff9f9f;
                    color: #b00020;
                    border-radius: 4px;
                    font-weight: bold;
                }}
            </style>
        </head>
        <body>
            <div class="page">
                {top_buttons_html}

                <a class="back" href="/etl-meta/?meta_type={escape(meta_type)}">← 목록으로</a>

                <h2>신규 DAG 설정 등록</h2>

                <div class="tabs">
                    {tabs_html}
                </div>

                <h3>{escape(META_SCHEMA)}.{escape(table_name)}</h3>

                {error_html}

                <form method="post" novalidate>
                    <input type="hidden" name="csrf_token" value="{csrf_token}">

                    {form_html}

                    <button class="btn" type="submit">저장</button>
                </form>
            </div>
        </body>
        </html>
        """

        return html


class EtlMetaPlugin(AirflowPlugin):
    name = "etl_meta_plugin"

    appbuilder_views = [
        {
            "name": "ETL Meta",
            "category": "Admin",
            "view": EtlMetaView(),
        }
    ]
