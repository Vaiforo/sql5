import psycopg
from datetime import date
from config import DB_CONN_STR


def fill_structured_table(start_date: str | date,
                          end_date: str | date,
                          conn_info: str | None = None):
    if conn_info is None:
        conn_info = DB_CONN_STR

    if isinstance(start_date, str):
        start_date = date.fromisoformat(start_date)
    if isinstance(end_date, str):
        end_date = date.fromisoformat(end_date)

    sql = "select s_psql_dds.fn_etl_data_load(%s, %s);"

    with psycopg.connect(conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start_date, end_date))
        conn.commit()

    print(
        f"[I] Структурированная таблица заполнена за период "
        f"{start_date} - {end_date}"
    )
