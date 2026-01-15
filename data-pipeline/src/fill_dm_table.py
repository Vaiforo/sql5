from datetime import date
import psycopg

from config import DB_CONN_STR


def fill_dm_table(start_dt: str | date, end_dt: str | date, conn_str: str | None = None) -> None:
    if conn_str is None:
        conn_str = DB_CONN_STR

    if isinstance(start_dt, str):
        start_dt = date.fromisoformat(start_dt)
    if isinstance(end_dt, str):
        end_dt = date.fromisoformat(end_dt)

    sql = "select s_psql_dds.fn_dm_data_load(%s, %s);"

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (start_dt, end_dt))
        conn.commit()

    print(f"[I] DM заполнен: {start_dt} .. {end_dt}")
