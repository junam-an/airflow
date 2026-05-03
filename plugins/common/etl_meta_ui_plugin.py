from __future__ import annotations

import json
from datetime import datetime
from html import escape

from flask import request, redirect
from flask_appbuilder import BaseView, expose
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.postgres.hooks.postgres import PostgresHook


META_POSTGRES_CONN_ID = "postgres_conn"
META_SCHEMA = "public"

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

        skip_columns = {
            "id",
            "created_at",
            "updated_at",
        }

        insert_columns = []

        for col in columns:
            name = col["name"]
            default = col["column_default"]

            if name in skip_columns:
                continue

            if default and "nextval" in default:
                continue

            insert_columns.append(col)

        return insert_columns

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

        # 특정 항목 선택 시
        if search_column and search_column in column_names:
            where_sql = f'WHERE "{search_column}"::text {operator} %s'
            return where_sql, [value]

        # 전체 항목 검색 시
        conditions = []

        for col in column_names:
            conditions.append(f'"{col}"::text {operator} %s')

        where_sql = "WHERE " + " OR ".join(conditions)
        params = [value] * len(column_names)

        return where_sql, params

    @expose("/")
    def list(self):
        meta_type = self._get_meta_type()
        table_name = self._get_table_name(meta_type)

        search_column = request.args.get("search_column", "")
        search_text = request.args.get("search_text", "")

        hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)

        columns = self._get_columns(table_name)
        column_names = [c["name"] for c in columns]

        if search_column not in column_names:
            search_column = ""

        select_cols = ", ".join([f'"{c}"' for c in column_names])

        where_sql, search_params = self._build_search_sql(
            column_names=column_names,
            search_column=search_column,
            search_text=search_text,
        )

        order_column = None

        for candidate in ["created_at", "updated_at", "id", "dag_id", "source_table", "target_table"]:
            if candidate in column_names:
                order_column = candidate
                break

        if order_column:
            order_sql = f'ORDER BY "{order_column}" DESC'
        else:
            order_sql = ""

        rows = hook.get_records(
            f"""
            SELECT {select_cols}
            FROM {META_SCHEMA}.{table_name}
            {where_sql}
            {order_sql}
            LIMIT 100
            """,
            parameters=tuple(search_params),
        )

        tabs_html = self._render_tabs(meta_type)

        search_area_html = self._build_search_area(
            meta_type=meta_type,
            column_names=column_names,
            search_column=search_column,
            search_text=search_text,
        )

        header_html = "".join(
            f"<th>{escape(col)}</th>"
            for col in column_names
        )

        row_html = ""

        for row in rows:
            row_html += "<tr>"
            for value in row:
                row_html += f"<td>{escape(str(value)) if value is not None else ''}</td>"
            row_html += "</tr>"

        html = f"""
        <html>
        <head>
            <style>
                .page {{
                    padding: 20px;
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
            </style>
        </head>
        <body>
            <div class="page">
                <h2>ETL Meta 관리</h2>

                <div class="tabs">
                    {tabs_html}
                </div>

                <h3>{escape(META_SCHEMA)}.{escape(table_name)}</h3>

                {search_area_html}

                <a class="btn" href="/etl-meta/new?meta_type={escape(meta_type)}">
                    신규 DAG 설정 등록
                </a>

                <h3>최근 등록된 설정</h3>

                <table>
                    <thead>
                        <tr>{header_html}</tr>
                    </thead>
                    <tbody>
                        {row_html}
                    </tbody>
                </table>
            </div>
        </body>
        </html>
        """

        return html

    @expose("/new", methods=["GET", "POST"])
    def new(self):
        meta_type = self._get_meta_type()
        table_name = self._get_table_name(meta_type)

        insert_columns = self._get_insert_columns(table_name)

        if request.method == "POST":
            hook = PostgresHook(postgres_conn_id=META_POSTGRES_CONN_ID)

            col_names = []
            values = []

            for col in insert_columns:
                name = col["name"]
                data_type = col["data_type"]

                raw_value = request.form.get(name)
                converted_value = self._convert_value(raw_value, data_type)

                if converted_value is not None:
                    col_names.append(name)
                    values.append(converted_value)

            if not col_names:
                raise Exception("No input values were provided.")

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

        for col in insert_columns:
            name = col["name"]
            data_type = col["data_type"]
            nullable = col["is_nullable"]

            required = "required" if nullable == "NO" else ""

            if data_type in ("json", "jsonb"):
                input_html = f"""
                <textarea name="{escape(name)}" rows="5" {required}></textarea>
                """
            elif data_type == "boolean":
                input_html = f"""
                <select name="{escape(name)}">
                    <option value=""></option>
                    <option value="true">true</option>
                    <option value="false">false</option>
                </select>
                """
            else:
                input_html = f"""
                <input type="text" name="{escape(name)}" {required}>
                """

            form_html += f"""
            <div class="form-row">
                <label>
                    {escape(name)}
                    <span class="type">({escape(data_type)})</span>
                </label>
                {input_html}
            </div>
            """

        tabs_html = self._render_tabs(meta_type)

        html = f"""
        <html>
        <head>
            <style>
                .page {{
                    padding: 20px;
                    max-width: 900px;
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
                    margin-bottom: 14px;
                }}

                label {{
                    display: block;
                    font-weight: bold;
                    margin-bottom: 5px;
                }}

                .type {{
                    color: #777;
                    font-weight: normal;
                    font-size: 12px;
                }}

                input, textarea, select {{
                    width: 100%;
                    padding: 8px;
                    border: 1px solid #ccc;
                    border-radius: 4px;
                    font-size: 13px;
                }}

                .btn {{
                    padding: 9px 16px;
                    background-color: #017cee;
                    color: white;
                    border: none;
                    border-radius: 4px;
                    cursor: pointer;
                }}

                .back {{
                    display: inline-block;
                    margin-bottom: 15px;
                }}
            </style>
        </head>
        <body>
            <div class="page">
                <a class="back" href="/etl-meta/?meta_type={escape(meta_type)}">← 목록으로</a>

                <h2>신규 DAG 설정 등록</h2>

                <div class="tabs">
                    {tabs_html}
                </div>

                <h3>{escape(META_SCHEMA)}.{escape(table_name)}</h3>

                <form method="post">
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