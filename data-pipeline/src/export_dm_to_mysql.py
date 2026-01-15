from datetime import date
import psycopg
import mysql.connector

from config import (
    DB_CONN_STR,
    MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD
)


def export_dm_to_mysql(start_dt: str | date, end_dt: str | date) -> None:
    pg_conn_str = DB_CONN_STR

    if isinstance(start_dt, str):
        start_dt = date.fromisoformat(start_dt)
    if isinstance(end_dt, str):
        end_dt = date.fromisoformat(end_dt)

    select_sql = """
        select
            id,
            src_id,
            car_id,
            owner_name,
            brand,
            model,
            year,
            vin,
            mileage,
            last_service_date,
            issue_description,
            service_cost,
            cleaned_at,
            owner_id,
            brand_id,
            model_id,
            year_id,
            vin_id,
            issue_id
        from s_psql_dds.v_dm_task
        where last_service_date between %s and %s
        order by id
    """

    with psycopg.connect(pg_conn_str) as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(select_sql, (start_dt, end_dt))
            rows = cur.fetchall()

    if not rows:
        print(f"[I] Нет данных в v_dm_task за период {start_dt} .. {end_dt}")
        return

    my_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DB,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
    )

    delete_sql = """
        delete from t_dm_task
        where last_service_date between %s and %s
    """

    insert_sql = """
        insert into t_dm_task (
            id,
            src_id,
            car_id,
            owner_name,
            brand,
            model,
            year,
            vin,
            mileage,
            last_service_date,
            issue_description,
            service_cost,
            cleaned_at,
            owner_id,
            brand_id,
            model_id,
            year_id,
            vin_id,
            issue_id
        ) values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    try:
        with my_conn.cursor() as cur:
            cur.execute(delete_sql, (start_dt, end_dt))
            cur.executemany(insert_sql, rows)
        my_conn.commit()
    finally:
        my_conn.close()

    print(
        f"[I] Перекладка в MySQL OK: строк={len(rows)} период={start_dt}..{end_dt}")
