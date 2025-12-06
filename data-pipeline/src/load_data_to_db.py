import math

import pandas as pd
import psycopg
from config import DB_CONN_STR


def load_data_to_db(df: pd.DataFrame,
                    conn_info: str | None = None,
                    table_name: str = "s_psql_dds.t_sql_source_unstructured"):
    if conn_info is None:
        conn_info = DB_CONN_STR

    cols = [
        "car_id",
        "owner_name",
        "brand",
        "model",
        "year",
        "vin",
        "mileage",
        "last_service_date",
        "issue_description",
        "service_cost",
    ]

    records = []
    for _, row in df[cols].iterrows():
        rec = []
        for col in cols:
            val = row[col]
            if val is None or (isinstance(val, float) and math.isnan(val)):
                rec.append(None)
            else:
                rec.append(str(val))
        records.append(rec)

    if not records:
        print("[E] Нечего загружать в неструктурированную таблицу")
        return

    insert_sql = f"""
        insert into {table_name} (
            car_id,
            owner_name,
            brand,
            model,
            year,
            vin,
            mileage,
            last_service_date,
            issue_description,
            service_cost
        )
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    with psycopg.connect(conn_info) as conn:
        with conn.cursor() as cur:
            cur.execute("truncate table s_psql_dds.t_sql_source_unstructured;")
            cur.executemany(insert_sql, records)
        conn.commit()

    print(f"[I] Загружено {len(records)} строк в {table_name}")
